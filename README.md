# LogFilter<br>
LogFilter的功能：<br>
  １、从kafka指定的topic中消费消息；<br>
  ２、将消息进行过滤(丢掉不要的，然后把剩下的再放入到另一个队列中）；<br>
  ３、在第２步的基础上再把error级别的消息拿出来，单独放到一个redis队列中；<br>
