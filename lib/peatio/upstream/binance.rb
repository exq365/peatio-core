# frozen_string_literal: true

require "faye/websocket"
require "em-http-request"
require "json"
require "openssl"
require "rbtree"
require "pp"

# Module provides access to trading on Binance upstream with following
# features:
#
# * Remote orderbooks objects that syncs in background and provides information
#   about current orderbook depth.
# * Trader that allows to execute given order with timeout and monitor its
#   state.
#
# All API provided by module is async.
class Peatio::Upstream::Binance
  include Peatio::Bus

  require_relative "binance/orderbook"
  require_relative "binance/client"
  require_relative "binance/trader"
  require_relative "binance/k_line"
  require_relative "binance/trade_book"

  # @return [Client]
  attr_accessor :client

  # @return [Trader]
  attr_accessor :trader

  # Creates new upstream and initializes Binance API client.
  # Require configuration to work.
  #
  # Trader can be used immediately after object creation and can be accessed
  # as +binance.trader+.
  #
  # @see Client
  # @see Trader
  def initialize
    @client = Client.new
    @trader = Trader.new(@client)
  end

  # Connects to Binance and start to stream orderbook data.
  #
  # Method is non-blocking and orderbook data is available only after stream
  # is successfully connected.
  #
  # To subscribe on open event use +on(:open)+ callback to register block
  # that will receive +orderbooks+ object.
  #
  # Method should be invoked inside +EM.run{}+ loop
  #
  # @example
  #   EM.run {
  #     upstream = Peatio::Upstream::Binance.new
  #     binance.start(["tusdbtc"])
  #     binance.on(:open) { |orderbooks|
  #       tusdbtc = orderbooks["tusdbtc"]
  #       # ...
  #     }
  #
  #     binance.on(:error) { |message|
  #       puts(message)
  #     }
  #   }
  #
  # @param markets [Array<String>] List of markets to listen.
  # @see on
  # @return [self]
  def start!(markets)
    orderbooks = {}
    klines = {}
    tradebooks = {}

    markets.each do |symbol|
      orderbooks[symbol] = Orderbook.new
      klines[symbol] = KLine.new
      tradebooks[symbol] = TradeBook.new
    end

    streams = markets.product(['depth', 'ticker', 'trade'] + KLine.kline_streams)
      .map { |e| e.join("@") }.join("/")

    @stream = @client.connect_public_stream!(streams)

    @stream.on :open do |event|
      logger.info "public streams connected: " + streams

      total = markets.length
      markets.each do |symbol|

        load_orderbook(symbol, orderbooks[symbol]) {
          total -= 1
          if total == 0
            emit(:orderbook_open, orderbooks)
          end
        }

        load_tradebook(symbol, tradebooks[symbol]) {
          total -= 1
          if total == 0
            emit(:tradebook_open, tradebooks)
          end
        }

        load_kline(symbol, klines[symbol]) {
          emit(:kline_open, klines)
        }

      end
    end

    @stream.on :message do |message|
      payload = JSON.parse(message.data)

      data = payload["data"]
      symbol, stream = payload["stream"].split("@")

      case stream
      when "depth"
        process_depth_diff(data, symbol, orderbooks)
      when "ticker"
        emit(:ticker_message,
              process_h24_data(data, symbol))
      when -> (s) { s.include?('kline') }
        period = KLine.non_humanize_period(stream.split("_")[1])
        emit(:kline_message,
             process_kline_data(data["k"], symbol, period, klines[symbol]))
      when 'trade'
          emit(:trade_message,
               process_tradebook(data, symbol))
      end
    end

    @stream.on :error do |message|
      logger.error(message)
      emit(:error, message)
    end

    self
  end

  # Stop listening streams.
  #
  # After calling this method orderbooks will no longer be updated.
  def stop
    @stream.close
  end

  protected

  def self.logger
    logger = Peatio::Logger.logger
    logger.progname = "binance"
    return logger
  end

  def logger
    self.class.logger
  end

  private

  def load_tradebook(symbol, tradebook)
    request = @client.trades_snapshot(symbol, 100)

    request.errback {
      logger.fatal "unable to request market trades for %s" % symbol

      emit(:error)
    }

    request.callback {
      if request.response_header.status != 200
        logger.fatal(
            "unexpected HTTP status code from binance: " \
          "#{request.response_header.status} #{request.response}"
        )

        emit(:error)

        next
      end

      payload = JSON.parse(request.response)

      logger.info "[#{symbol}] trades data loaded: " \
                  "(#{payload.length} trades)"

      payload.each do |d|
        type = d['isBuyerMaker'] ? 'buy' : 'sell'
        tradebook.add(d['id'], type, d['time'].to_i / 1000, d['price'] , d['qty'])
      end

      yield if block_given?
    }

  end

  def process_tradebook(d, symbol)
    type = d['m'] ? 'buy' : 'sell'
    {symbol: symbol,
     data: {
         tid: d['t'],
         type: type,
         date: d['E'].to_i / 1000,
         price: d['p'],
         amount: d['q']
     }
    }
  end

  def process_depth_diff(data, symbol, orderbooks)
    orderbook = orderbooks[symbol]
    generation = data["u"]

    asks, bids = update_orderbook(
      orderbook,
      data["a"],
      data["b"],
      generation
    )

    logger.debug "[#{symbol}] ##{generation} orderbook event: " \
      "%+d asks (%f min), %+d bids (%f max)" % [
        asks,
        orderbook.min_ask,
        bids,
        orderbook.max_bid,
      ]
  end

  def process_kline_data(data, symbol, peroid, kline)
    { symbol: symbol,
      period: peroid,
      data: kline.filter(data["t"], data["o"], data["h"], data["l"], data["c"], data["v"])}
  end

  def process_h24_data(data, symbol)
    { symbol: symbol,
      data: {
              low:       data['l'].to_d,
              high:      data['h'].to_d,
              last:      data['c'].to_d,
              volume:    data['v'].to_d,
              open:      data['o'].to_d,
              sell:      data['a'].to_d,
              buy:       data['b'].to_d,
              avg_price: data['w'].to_d,
              price_change_percent: data['P']
            }
    }
  end


  def load_kline(symbol, kline)
    KLine::AVAILABLE_POINT_PERIODS.each do |period|
      request = @client.kline_data(symbol, KLine.humanize_period(period))

      request.errback {
        logger.fatal "unable to request market kline for %s - %s" % [symbol, KLine.humanize_period(period)]

        emit(:error)
      }

      request.callback {
        if request.response_header.status != 200
          logger.fatal(
              "unexpected HTTP status code from binance: " \
          "#{request.response_header.status} #{request.response}"
          )

          emit(:error)

          next
        end

        payload = JSON.parse(request.response)
        data = payload.map{|v| v.first(6)}

        logger.info "[#{symbol}] ##{KLine.humanize_period(period)} kline data loaded: " \
                  "(#{data.length} data)"

        data.each do |(open_time, open, high, low, close, volume)|
          kline.add(period, open_time, open, high, low, close, volume)
        end

        yield if block_given?
      }
    end
  end

  def load_orderbook(symbol, orderbook)
    request = @client.depth_snapshot(symbol)

    request.errback {
      logger.fatal "unable to request market depth for %s" % symbol

      emit(:error)
    }

    request.callback {
      if request.response_header.status != 200
        logger.fatal(
          "unexpected HTTP status code from binance: " \
          "#{request.response_header.status} #{request.response}"
        )

        emit(:error)

        next
      end

      payload = JSON.parse(request.response)

      generation = payload["lastUpdateId"]
      bids = payload["bids"]
      asks = payload["asks"]

      logger.info "[#{symbol}] ##{generation} orderbook snapshot loaded: " \
                  "(#{bids.length} bids, #{asks.length} asks)"

      orderbook.commit(generation) {
        payload["bids"].each do |(price, volume)|
          orderbook.bid(price, volume, generation)
        end

        payload["asks"].each do |(price, volume)|
          orderbook.ask(price, volume, generation)
        end
      }

      yield if block_given?
    }
  end

  def update_orderbook(orderbook, asks, bids, generation)
    asks_diff = 0
    asks.each do |(price, volume)|
      asks_diff += orderbook.ask(price, volume, generation)
    end

    bids_diff = 0
    bids.each do |(price, volume)|
      bids_diff += orderbook.bid(price, volume, generation)
    end

    return asks_diff, bids_diff
  end
end
