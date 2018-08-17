module Peatio::Upstream::Binance
  class Trader
    def logger
      Peatio::Upstream::Binance.logger
    end

    def initialize(client)
      @client = client
    end

    class Trade
      def initialize
        @callbacks = {}
      end

      def emit(message, *args)
        @callbacks[message].yield(*args) unless @callbacks[message].nil?
      end

      def on(message, &block)
        @callbacks[message] = block
      end
    end

    def process(buyer, seller, price, amount)
    end

    def submit_order(timeout:, order:)
      logger.info "submitting new order: [#{order[:symbol].downcase}] " \
                  "#{order[:type]} #{order[:side]} " \
                  "amount=#{order[:quantity]} price=#{order[:price]}"
      trade = Trade.new

      request = @client.submit_order(order)
      request.errback {
        trade.emit(:error, request) unless trade.nil?
      }

      request.callback {
        if request.response_header.status >= 300
          trade.emit(:error, request)
        else
          payload = JSON.parse(request.response)

          trade.emit(:submit, payload["orderId"])
        end
      }

      trade
    end
  end
end
