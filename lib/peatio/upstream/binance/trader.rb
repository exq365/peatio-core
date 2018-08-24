module Peatio::Upstream::Binance
  class Trader < Peatio::Wire
    class Trade < Peatio::Wire
    end

    def on(message, &block)
      super(message, &block)
    end

    def logger
      Peatio::Upstream::Binance.logger
    end

    def initialize(client)
      @client = client
      @ready = false
    end

    def process(buyer, seller, price, amount)
    end

    def order(timeout:, order:)
      trade = Trade.new

      if @ready
        submit_order(timeout, order, trade)
      else
        on(:ready) {
          submit_order(timeout, order, trade)
        }
      end

      trade
    end

    private

    def submit_order(timeout, order, trade)
      logger.info "[#{order[:symbol].downcase}] submitting new order: " \
                  "#{order[:type]} #{order[:side]} " \
                  "amount=#{order[:quantity]} price=#{order[:price]}"

      request = @client.submit_order(order)

      request.errback {
        trade.emit(:error, request)
      }

      request.callback {
        if request.response_header.status >= 300
          trade.emit(:error, request)
        else
          payload = JSON.parse(request.response)

          id = payload["orderId"]

          logger.info "[#{order[:symbol].downcase}] ##{id} order submitted: " \
                      "#{order[:type]} #{order[:side]} " \
                      "amount=#{order[:quantity]} price=#{order[:price]}"

          trade.emit(:submit, id)
        end
      }

      trade
    end
  end
end
