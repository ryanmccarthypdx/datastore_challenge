require 'spec_helper'
require 'query'

describe Query do
  describe '.initialize' do
    context 'acceptance tests' do
      it 'should set all instance vars correctly if given query argvs' do
        pending("don't forget")
        fail
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
end
