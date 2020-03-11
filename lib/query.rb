require_relative 'data_store'
require_relative 'state_map'
require_relative 'uniq_store'
require_relative '../config/psv_headers'

class QueryError < StandardError
  # TODO: maybe add some --help text here?
end

class Query
  attr_accessor :selects, :orders, :filters
  def initialize(argv = [])
    raise QueryError, "Wrong number of args received: #{argv.join(' ')}" unless argv.count.even?
    while argv.any?
      flag, val = argv.shift(2)
      case flag
      when "-s"
        set_selects(val)
      when "-o"
        set_orders(val)
      when "-f"
        add_to_filters(val)
      else
        raise QueryError, "Unknown flag: #{flag}"
      end
    end
  end

    def set_selects(val)

    end

    def set_orders(val)

    end

    def add_to_filters(val)

    end
end
