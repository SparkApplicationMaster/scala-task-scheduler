package ru.eglotov

import java.time.LocalDateTime
import java.util.concurrent.{ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{Cancellable, Scheduler}
import akka.event.LoggingAdapter
import akka.util.Helpers
import com.typesafe.config.Config

import scala.annotation.tailrec
import scala.collection.{immutable, mutable}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import akka.util.Helpers._
import scala.util.control.NonFatal
import akka.util.Unsafe.{instance => unsafe}
import java.time.temporal.ChronoUnit

class SortedTimesScheduler(config: Config,
                           log: LoggingAdapter,
                           threadFactory: ThreadFactory)
  extends Scheduler {

  import SortedTimesScheduler._


  implicit def dateTimeOrdering: Ordering[LocalDateTime] = Ordering.fromLessThan(_ isBefore _)

  // queue is sorted map with time as key, for equal time tasks are queued internally
  private val queue = mutable.TreeMap[LocalDateTime, mutable.Queue[TaskHolder]]()

  private def enqueueTask(task: TaskHolder, executionDateTime: LocalDateTime): Unit = {
    if (!queue.contains(executionDateTime))
      queue += ((executionDateTime, mutable.Queue(task)))
    else
      queue(executionDateTime).enqueue(task)
  }

  private val start = System.nanoTime

  val TickDuration =
    Duration(config.getDuration("akka.scheduler.tick-duration", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      .requiring(_ >= 10.millis || !Helpers.isWindows, "minimum supported akka.scheduler.tick-duration on Windows is 10ms")
      .requiring(_ >= 1.millis, "minimum supported akka.scheduler.tick-duration is 1ms")

  private val tickNanos = TickDuration.toNanos

  private def schedule(ec: ExecutionContext, r: Runnable, executionDateTime: LocalDateTime): Cancellable = {
    val delay = delayFromDateTime(executionDateTime)
    if (delay <= Duration.Zero) {
      if (stopped.get != null) throw new IllegalStateException("cannot enqueue after timer shutdown")
      ec.execute(r)
      NotCancellable
    } else if (stopped.get != null) {
      throw new IllegalStateException("cannot enqueue after timer shutdown")
    } else {
      val delayNanos = delay.toNanos
      checkMaxDelay(delayNanos)

      val ticks = (delayNanos / tickNanos).toInt
      val task = new TaskHolder(r, ticks, ec)

      enqueueTask(task, executionDateTime)

      if (stopped.get != null && task.cancel())
        throw new IllegalStateException("cannot enqueue after timer shutdown")
      task
    }
  }

  private val stopped = new AtomicReference[Promise[immutable.Seq[Runnable]]]
  private def stop(): Future[immutable.Seq[Runnable]] = {
    val p = Promise[immutable.Seq[Runnable]]()
    if (stopped.compareAndSet(null, p)) {
      // Interrupting the timer thread to make it shut down faster is not good since
      // it could be in the middle of executing the scheduled tasks, which might not
      // respond well to being interrupted.
      // Instead we just wait one more tick for it to finish.
      p.future
    } else Future.successful(Nil)
  }

  def scheduleOnce(executionDateTime: LocalDateTime, runnable: Runnable)(implicit executor: ExecutionContext) = {
    schedule(executor, runnable, executionDateTime)
  }

  override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext) = {
    scheduleOnce(dateTimeFromDelay(roundUp(delay)), runnable)
  }

  override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext) = ???

  private def dateTimeFromDelay(delay: FiniteDuration) = LocalDateTime.now().plusNanos(delay.toNanos)
  private def delayFromDateTime(dateTime: LocalDateTime) = LocalDateTime.now().until(dateTime, ChronoUnit.NANOS).nanos

  override def maxFrequency: Double = 1.second / TickDuration

  private def checkMaxDelay(delayNanos: Long): Unit =
    if (delayNanos / tickNanos > Int.MaxValue)
    // 1 second margin in the error message due to rounding
      throw new IllegalArgumentException(s"Task scheduled with [${delayNanos.nanos.toSeconds}] seconds delay, " +
        s"which is too far in future, maximum delay is [${(tickNanos * Int.MaxValue).nanos.toSeconds - 1}] seconds")

  private def roundUp(d: FiniteDuration): FiniteDuration = {
    val dn = d.toNanos
    val r = ((dn - 1) / tickNanos + 1) * tickNanos
    if (r != dn && r > 0 && dn > 0) r.nanos else d
  }

  private def waitNanos(nanos: Long): Unit = {
    // see http://www.javamex.com/tutorials/threads/sleep_issues.shtml
    val sleepMs = if (Helpers.isWindows) (nanos + 4999999) / 10000000 * 10 else (nanos + 999999) / 1000000
    try Thread.sleep(sleepMs) catch {
      case _: InterruptedException ⇒ Thread.currentThread.interrupt() // we got woken up
    }
  }

  private def startTick: Int = 0

  @volatile private var timerThread: Thread = threadFactory.newThread(new Runnable {

    var tick = startTick

    private def clearAll(): immutable.Seq[(Runnable)] = {
      val enqueued = queue.flatMap(x => x._2).toList
      queue.clear()
      enqueued
    }

    @tailrec
    private def executeCurrentTimeTasks(): Unit = {
      // if reached first dateTime in queue than execute and remove from queue
      if (!queue.firstKey.isAfter(LocalDateTime.now())) {
        if (queue.head._2.isEmpty)
          queue -= queue.firstKey
        else
          queue.head._2.dequeue().executeTask()

        executeCurrentTimeTasks()
      }
    }

    override final def run =
      try nextTick()
      catch {
        case t: Throwable ⇒
          log.error(t, "exception on STS’ timer thread")
          stopped.get match {
            case null ⇒
              val thread = threadFactory.newThread(this)
              log.info("starting new STS thread")
              try thread.start()
              catch {
                case e: Throwable ⇒
                  log.error(e, "STS cannot start new thread, ship’s going down!")
                  stopped.set(Promise successful Nil)
                  clearAll()
              }
              timerThread = thread
            case p ⇒
              assert(stopped.compareAndSet(p, Promise successful Nil), "Stop signal violated in STS")
              p success clearAll()
          }
          throw t
      }

    @tailrec final def nextTick(): Unit = {
      executeCurrentTimeTasks()
      val sleepTime = start + (tick * tickNanos) - System.nanoTime
      tick += 1

      if (sleepTime > 0) {
        waitNanos(sleepTime)
      }

      stopped.get match {
        case null ⇒ nextTick()
        case p ⇒
          assert(stopped.compareAndSet(p, Promise successful Nil), "Stop signal violated in STS")
          p success clearAll()
      }
    }
  })

  timerThread.start()
}


object SortedTimesScheduler {
  private[this] val taskOffset = unsafe.objectFieldOffset(classOf[TaskHolder].getDeclaredField("task"))

  /**
    * INTERNAL API
    */
  protected trait TimerTask extends Runnable with Cancellable

  /**
    * INTERNAL API
    */
  protected class TaskHolder(@volatile var task: Runnable, var ticks: Int, executionContext: ExecutionContext)
    extends TimerTask {

    @tailrec
    private final def extractTask(replaceWith: Runnable): Runnable =
      task match {
        case t @ (ExecutedTask | CancelledTask) ⇒ t
        case x ⇒ if (unsafe.compareAndSwapObject(this, taskOffset, x, replaceWith)) x else extractTask(replaceWith)
      }

    private[eglotov] final def executeTask(): Boolean = extractTask(ExecutedTask) match {
      case ExecutedTask | CancelledTask ⇒ false
      case other ⇒
        try {
          executionContext execute other
          true
        } catch {
          case _: InterruptedException ⇒ { Thread.currentThread.interrupt(); false }
          case NonFatal(e) ⇒ { executionContext.reportFailure(e); false }
        }
    }

    // This should only be called in execDirectly
    override def run(): Unit = extractTask(ExecutedTask).run()

    override def cancel(): Boolean = extractTask(CancelledTask) match {
      case ExecutedTask | CancelledTask ⇒ false
      case _ ⇒ true
    }

    override def isCancelled: Boolean = task eq CancelledTask
  }

  private[this] val CancelledTask = new Runnable { def run = () }
  private[this] val ExecutedTask = new Runnable { def run = () }

  private val NotCancellable: TimerTask = new TimerTask {
    def cancel(): Boolean = false
    def isCancelled: Boolean = false
    def run(): Unit = ()
  }

  private val InitialRepeatMarker: Cancellable = new Cancellable {
    def cancel(): Boolean = false
    def isCancelled: Boolean = false
  }
}
