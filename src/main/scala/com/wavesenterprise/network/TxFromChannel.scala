package com.wavesenterprise.network

import io.netty.channel.Channel

case class TxFromChannel(channel: Channel, txWithSize: TransactionWithSize)
