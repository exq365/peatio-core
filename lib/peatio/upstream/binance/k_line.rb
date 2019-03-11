class Peatio::Upstream::Binance::KLine

  AVAILABLE_POINT_PERIODS = [1, 5, 15, 30, 60, 120, 240, 360, 720, 1440, 4320, 10080].freeze

  HUMANIZED_POINT_PERIODS = {
      1 => '1m', 5 => '5m', 15 => '15m', 30 => '30m',                   # minuets
      60 => '1h', 120 => '2h', 240 => '4h', 360 => '6h', 720 => '12h',  # hours
      1440 => '1d', 4320 => '3d',                                       # days
      10080 => '1w'                                                     # weeks
  }.freeze

  class << self
    def humanize_period(period)
      HUMANIZED_POINT_PERIODS.fetch(period) do
        raise StandardError, "Not available period #{period}"
      end
    end

    def non_humanize_period(period)
      HUMANIZED_POINT_PERIODS.invert.fetch(period) do
        raise StandardError, "Not available period #{period}"
      end
    end


    def kline_streams
      AVAILABLE_POINT_PERIODS.map{|p| "kline_#{humanize_period(p)}"}
    end
  end

  def initialize
    @klines = Hash.new
  end


  class Entry
    attr_reader :open_time, :open, :high, :low, :close, :volume

    def initialize(open_time, open, high, low, close, volume)
      @open_time = open_time
      @open = open
      @high = high
      @low = low
      @close = close
      @volume = volume
    end

    def read_data
      [@open_time.to_i / 1000, @open.to_f, @high.to_f, @low.to_f, @close.to_f, @volume.to_f.round(4)]
    end
  end

  def add(period, open_time, open, high, low, close, volume)
    @klines[period] ||= Array.new
    @klines[period] <<  Entry.new(open_time, open, high, low, close, volume).read_data
  end

  def filter(open_time, open, high, low, close, volume)
    Entry.new(open_time, open, high, low, close, volume).read_data
  end


  def depth
    data = {}
    @klines.each { |period, points|
      data[period] = points
    }
    data
  end
end