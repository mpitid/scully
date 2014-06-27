
package scully

import org.zeromq.{ZContext, ZMQ}
import scala.collection.mutable.ListBuffer


object main {
  import scully.configuration._

  case class Options(
      endpoints: List[String] = Nil,
      socketType: Option[SocketType] = None,
      multipart: Option[String] = None,
      topic: String = "",
      hwm: Option[Long] = None,
      times: Boolean = false,
      mode: Mode = Blocking,
      timeout: Long = -1,
      threads: Int = 1
    )

  def main(args: Array[String]) {

    val opts = parseCLI(args.toList)

    val context = new ZContext()
    context.setIoThreads(opts.threads)
    val socketType = opts.socketType.get
    val socket = context.createSocket(socketType.code)
    opts.hwm.map(socket.setHWM)

    for (endpoint <- opts.endpoints)
      if (endpoint contains '*')
        socket bind endpoint
      else
        socket connect endpoint

    socketType match {
      case Push | Pub => sendLoop(socket, opts)
      case Pull => receiveLoop(socket, opts)
      case Sub =>
        socket.subscribe(opts.topic.getBytes("UTF-8"))
        receiveLoop(socket, opts)
    }

    opts.endpoints.foreach(socket.disconnect)
    context.destroy()
  }


  def receiveLoop(socket: ZMQ.Socket, options: Options) = {
    val receive = receiver(socket, options.mode, options.timeout,
      options.multipart.map(receiveMultipart(socket, _)).getOrElse(() => Option(socket.recvStr())))
    var continue = true
    val printer =
      if (options.times)
        (s: String) => println(System.currentTimeMillis + " " + s)
      else
        (s: String) => println(s)
    while (continue) {
      receive() match {
        case None => continue = false
        case Some(line) => printer(line)
      }
    }
  }

  def receiver(socket: ZMQ.Socket, mode: Mode, timeout: Long, receive: () => Option[String]): () => Option[String] =
    mode match {
      case Polling =>
        val poller = new ZMQ.Poller(1)
        poller.register(socket, ZMQ.Poller.POLLIN)
        () =>
          while (poller.poll(timeout) != 1) {}
          receive()
      case _ => receive
    }

  def receiveMultipart(socket: ZMQ.Socket, separator: String): () => Option[String] =
    () => {
      val buffer = ListBuffer[String](socket.recvStr())
      while (socket.hasReceiveMore)
        buffer.append(socket.recvStr())
      Some(buffer.mkString(separator))
  }

  def sendLoop(socket: ZMQ.Socket, options: Options) = {
    val send = sender(socket, options.mode, options.timeout,
      options.multipart.map(sendMultipart(socket, _)).getOrElse((s: String) => socket.send(s)))
    for (line <- scala.io.Source.stdin.getLines())
      send(line)
  }

  def sender(socket: ZMQ.Socket, mode: Mode, timeout: Long, send: (String) => Boolean) =
    mode match {
      case Polling =>
        val poller = new ZMQ.Poller(1)
        poller.register(socket, ZMQ.Poller.POLLOUT)
        (message: String) =>
          while (poller.poll(timeout) != 1) {}
          send(message)
      case _ => send
    }

  def sendMultipart(socket: ZMQ.Socket, separator: String): (String) => Boolean =
    (message: String) =>
      send(socket, message.split(separator).toList)

  @annotation.tailrec
  def send(socket: ZMQ.Socket, parts: List[String]): Boolean = parts match {
    case last :: Nil => socket.send(last)
    case h :: t => socket.sendMore(h) && send(socket, t)
    case Nil => throw new IllegalArgumentException("need at least one part")
  }

  def parseCLI(args: List[String]): Options =
    parseArg(args, Options()) match {
      case Right(opts) => opts
      case Left(error) =>
        System.err.println(s"Fatal error: $error\n")
        System.err.println(usage)
        System.exit(1).asInstanceOf[Options]
    }

  @annotation.tailrec
  def parseArg(args: List[String], opts: Options): Either[String, Options] = args match {
    case ("-t"|"--type") :: socket :: rest =>
      parseArg(rest, opts.copy(socketType = Some(SocketType(socket))))
    case ("-m"|"--multipart") :: separator :: rest =>
      parseArg(rest, opts.copy(multipart = Some(separator)))
    case ("--hwm") :: hwm :: rest =>
      parseArg(rest, opts.copy(hwm = Some(hwm.toLong)))
    case ("--topic") :: topic :: rest =>
      parseArg(rest, opts.copy(topic = topic))
    case ("--times") :: rest =>
      parseArg(rest, opts.copy(times = true))
    case ("--mode") :: mode :: rest =>
      parseArg(rest, opts.copy(mode = Mode(mode)))
    case ("--timeout") :: timeout :: rest =>
      parseArg(rest, opts.copy(timeout = timeout.toLong))
    case "--io-threads" :: threads :: rest =>
      parseArg(rest, opts.copy(threads = threads.toInt))
    case endpoint :: rest =>
      parseArg(rest, opts.copy(endpoints = endpoint :: opts.endpoints))
    case Nil =>
      if (opts.endpoints.isEmpty)
        Left("you must specify at least one endpoint")
      else if (opts.socketType.isEmpty)
        Left("you must specify a socket type")
      else Right(opts.copy(endpoints = opts.endpoints.reverse))
  }

  def usage: String =
    """usage: scully [OPTIONS] endpoint [endpoint ...]"""

}


object configuration {
  sealed abstract class SocketType(val value: String, val code: Int)
  case object Push extends SocketType("push", ZMQ.PUSH)
  case object Pull extends SocketType("pull", ZMQ.PULL)
  case object Pub extends SocketType("pub", ZMQ.PUB)
  case object Sub extends SocketType("sub", ZMQ.SUB)

  object SocketType {
    val types = List(Push, Pull, Pub, Sub)
    val mapping = types.map { t => (t.value, t) }.toMap.lift

    def apply(value: String): SocketType =
      mapping(value.toLowerCase) getOrElse {
        throw new IllegalArgumentException(
          s"unsupported socket type `$value`, pick one of ${types.map(_.value).mkString(", ")}")
      }
  }

  // Configure how data is sent/received:
  sealed trait Mode
  case object Blocking extends Mode
  case object Polling extends Mode

  object Mode {
    def apply(value: String): Mode =
      value.toLowerCase match {
        case "b" | "block" | "blocking" => Blocking
        case "p" | "poll" | "polling" => Polling
        case _ => throw new IllegalArgumentException(
          s"unsupported mode `$value`, pick one of block, poll")
      }
  }
}
