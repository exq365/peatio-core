class Peatio::Upstream::Binance::TradeBook
  def initialize
    @trades_history = Array.new
  end

  class Entry
    attr_reader :tid, :type, :date, :price, :amount

    def initialize(tid, type, date, price, amount)
      @tid = tid
      @type = type
      @date = date
      @price = price
      @amount = amount
    end
  end

  def add(tid, type, date, price, amount)
    @trades_history << Entry.new(tid, type, date, price, amount)
  end

  def fetch(size = 100)
    history = []

    @trades_history.each do |h|
      history.insert(0, {
          tid: h.tid,
          type: h.type,
          date: h.date,
          price: h.price,
          amount: h.amount
      })
      break if history.length >= size
    end

    history
  end

end
