require 'spec_helper'
require 'query'
require 'ingester'

describe Query do
  let(:test_argv) { [
            '-s', PSV_HEADERS[2..3].join(','),
            '-o', PSV_HEADERS[2..3].reverse.join(','),
            '-s', PSV_HEADERS[0..1].join(','),
            '-o', PSV_HEADERS[0..1].reverse.join(','),
            '-f', 'PROVIDER=warner bros',
            '-f', 'DATE=2020-03-11'
          ] }
  let(:test_query) { Query.new }

  describe '.initialize' do
    context 'acceptance test' do
      it 'should set all instance vars correctly if given query argvs' do
        test_query = Query.new(test_argv)
        expect(test_query.selects).to eq(PSV_HEADERS[0..1])
        expect(test_query.orders).to eq(PSV_HEADERS[0..1].reverse)
        expect(test_query.filters).to eq({'PROVIDER' => 'warner bros', 'DATE' => '2020-03-11'})
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

  context 'helper methods' do
    context 'initialize helpers' do
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

    describe '#fetch_with_filters' do
      context 'when the filter_hash has values that can be found' do
        let(:test_filter_hash) { {'a column name' => 'an index key', 'another column name' => 'another index key'} }
        before do
          allow(Index).to receive(:fetch_ids)
            .with(column_name: 'a column name', index_key: 'an index key')
            .and_return([1,2,3,4])
          allow(Index).to receive(:fetch_ids)
            .with(column_name: 'another column name', index_key: 'another index key')
            .and_return([3,4,5,6])
        end

        it 'requests that the DataStore get_all of the ids that match all filters' do
          expect(DataStore).to receive(:get_bulk).with({"./data/test/data_store_0.pstore"=>[3, 4]})
          test_query.fetch_with_filters(test_filter_hash)
        end
      end

      context 'when the filter_hash has values that cannot satisfy the filter(s)' do
        let(:test_filter_hash) { {'a column name' => 'an index key'} }
        before do
          allow(Index).to receive(:fetch_ids)
            .with(column_name: 'a column name', index_key: 'an index key')
            .and_return([])
        end
        it 'returns an empty array' do
          expect(test_query.fetch_with_filters(test_filter_hash)).to eq([])
        end
      end
    end

    context 'in-place result mutators' do
      let(:test_input_results) { [
          ["stb2", "An OK Movie", "I should be second"],
          ["stb1", "Better Movie", "I should be third"],
          ["stb1", "An OK Movie", "I should be first"]
        ] }

      describe '#apply_orders_in_place!' do
        let(:order_array) { PSV_HEADERS[0..1].reverse } # ie, TITLE,STB

        it 'sorts as-expected' do
          ordered_results = test_query.apply_orders_in_place!(test_input_results, order_array)
          expect(ordered_results.map{|r| r[2]}).to eq(["I should be first", "I should be second", "I should be third"])
        end

        it 'sorts the results in place' do
          ordered_results = test_query.apply_orders_in_place!(test_input_results, order_array)
          expect(ordered_results).to equal(test_input_results) # identity matcher
        end
      end

      describe '#apply_selects_in_place!' do
        let(:selects_array) { PSV_HEADERS[0..1].reverse } # ie, TITLE,STB
        it 'only returns the selected fields' do
          selected_results = test_query.apply_selects_in_place!(test_input_results, selects_array)
          expect(selected_results).to eq([['An OK Movie', 'stb2'], ['Better Movie', 'stb1'], ['An OK Movie', 'stb1']])
        end

        it 'performs the select in-place on the results' do
          selected_results = test_query.apply_selects_in_place!(test_input_results, selects_array)
          expect(selected_results).to equal(test_input_results) # identity matcher
        end
      end
    end
  end

  describe '#perform' do
    context 'acceptance test' do
      before { Ingester.new('./spec/support/query_acceptance_test.psv').ingest }

      it 'responds as-expected' do
        result = Query.new(test_argv).perform
        expect(result).to eq([["stb7", "Absolution"], ["stb5", "Metropolis"]])
      end
    end

    it 'short circuits to empty array if filters are contradictory' do
      allow(test_query).to receive(:filters).and_return({"DATE" => "2001-01-01", "DATE" => "2001-12-31"}) # illustrative purposes only; just need any? to be truthy
      allow(test_query).to receive(:fetch_with_filters).and_return([])
      expect(test_query.perform).to eq([])
    end
  end
end
