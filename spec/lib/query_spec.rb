require 'spec_helper'
require 'query'

describe Query do
  describe '.initialize' do
    context 'acceptance test' do
      it 'should set all instance vars correctly if given query argvs' do
        argv =  [ '-s', PSV_HEADERS[0..1].join(','),
                  '-o', PSV_HEADERS[0..1].reverse.join(','),
                  '-s', PSV_HEADERS[2..3].join(','),
                  '-o', PSV_HEADERS[2..3].reverse.join(','),
                  '-f', 'STB=stb01',
                  '-f', 'DATE=2020-03-11'
                ]
        test_query = Query.new(argv)
        expect(test_query.selects).to eq(PSV_HEADERS[2..3])
        expect(test_query.orders).to eq(PSV_HEADERS[2..3].reverse)
        expect(test_query.filters).to eq({'STB' => 'stb 01', 'DATE' => '202 0-03-11'})
      end
    end

    it 'should not set any instance vars if not given any query argvs' do
      expect(Query.new([]).instance_variables).to eq([])
    end

    it 'raises an error if given the wrong number of args' do
      expect{ Query.new(['-f', 'DATE=2020-03-11', '-s']) }.to raise_error(QueryError, "Wrong number of args received: -f DATE=2020-03-11 -s")
    end

    it 'raises an error if given a bad flag' do
      expect{ Query.new(['-x', 'harglebargle']) }.to raise_error(QueryError, "Unknown flag: -x")
    end

    it 'calls the correct instance_var setters' do
      allow_any_instance_of(Query).to receive(:set_orders)
      allow_any_instance_of(Query).to receive(:set_selects)
      allow_any_instance_of(Query).to receive(:add_to_filters)
      test_query = Query.new(['-f', 'some_filter_params', '-s', 'select_params', '-o', 'order_params', '-f', 'more_filter_params'])
      expect(test_query).to have_received(:set_orders).with('order_params')
      expect(test_query).to have_received(:set_selects).with('select_params')
      expect(test_query).to have_received(:add_to_filters).with('some_filter_params')
      expect(test_query).to have_received(:add_to_filters).with('more_filter_params')
    end
  end

  context 'instance methods' do
    let(:test_query) { Query.new }

    describe '#set_selects' do
      it 'raises an error if passed a bad value' do
        expect{ test_query.set_selects("HARGLE") }.to raise_error(QueryError, "HARGLE is not a valid column!")
      end

      it 'set the value passed as an array' do
        test_query.set_selects(PSV_HEADERS[0..3].join(','))
        expect(test_query.selects).to eq(PSV_HEADERS[0..3])
      end

      it 'overwrites the initial values with new values if passed multiple times' do
        test_query.set_selects(PSV_HEADERS[0..1].join(','))
        test_query.set_selects(PSV_HEADERS[2..3].join(','))
        expect(test_query.selects).to eq(PSV_HEADERS[2..3])
      end
    end

    describe '#set_orders' do
      it 'raises an error if passed a bad value' do
        expect{ test_query.set_orders("BARGLE") }.to raise_error(QueryError, "BARGLE is not a valid column!")
      end

      it 'set the value passed as an array' do
        test_query.set_orders(PSV_HEADERS[0..3].join(','))
        expect(test_query.orders).to eq(PSV_HEADERS[0..3])
      end

      it 'overwrites the initial values with new values if passed multiple times' do
        test_query.set_orders(PSV_HEADERS[0..1].join(','))
        test_query.set_orders(PSV_HEADERS[2..3].join(','))
        expect(test_query.orders).to eq(PSV_HEADERS[2..3])
      end
    end

    describe '#add_to_filters' do
      it 'raises an error if passed a bad value' do
        expect{ test_query.add_to_filters("NARGLE='Goldfinger'") }.to raise_error(QueryError, "NARGLE is not a valid column!")
      end

      it 'adds the given filter to filters as a key-value pair' do
        test_query.add_to_filters("#{PSV_HEADERS[0]}=some value")
        expect(test_query.filters).to eq({PSV_HEADERS[0] => 'some value'})
      end

      it 'overwrites a filter if given the same key' do
        test_query.add_to_filters("#{PSV_HEADERS[0]}=some value")
        test_query.add_to_filters("#{PSV_HEADERS[0]}=a different value")
        expect(test_query.filters).to eq({PSV_HEADERS[0] => 'a different value'})
      end

      it 'adds subsequent filters if they have different keys' do
        test_query.add_to_filters("#{PSV_HEADERS[0]}=some value")
        test_query.add_to_filters("#{PSV_HEADERS[1]}=a different value")
        expect(test_query.filters).to eq({
            PSV_HEADERS[0] => 'some value',
            PSV_HEADERS[1] => 'a different value',
          })
      end
    end
  end
end
