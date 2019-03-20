class Peatio::Upstream::Binance::TradeBook
  def initialize
    @trades_history = Array.new
    @my_trades = Array.new
  end

  class Entry
    attr_reader :tid, :type, :date, :price, :amount, :ask_id, :bid_id

    def initialize(tid, type, date, price, amount, ask_id, bid_id)
      @tid = tid
      @type = type
      @date = date
      @price = price
      @amount = amount
      @ask_id = ask_id
      @bid_id = bid_id
    end
  end

  def add(tid, type, date, price, amount, ask_id, bid_id)
    @trades_history << Entry.new(tid, type, date, price, amount, ask_id, bid_id)
  end

  def fetch(size = 100)
    history = []

    @trades_history.each do |h|
      history.insert(0, {
          tid: h.tid,
          type: h.type,
          date: h.date,
          price: h.price,
          amount: h.amount,
          ask_id: h.ask_id,
          bid_id: h.bid_id
      })
      break if history.length >= size
    end

    history
  end

  def add_my_trade(tid, type, date, price, amount, ask_id, bid_id)
    @my_trades << Entry.new(tid, type, date, price, amount, ask_id, bid_id)
                      .as_json
                      .symbolize_keys!
  end

  def fetch_my_trades
    @my_trades
  end
end
