require 'spec_helper'
require 'ingester'

describe Ingester do
  describe '.upsert_row' do
    let(:test_row) { {test_key: "test_value"} }
    let(:subject) { Ingester.upsert_row(test_row) }
    before do
      allow(StateMap).to receive(:increment_current_id).and_return(2)
      allow(DataStore).to receive(:create_new_record_from_row).with(id: 2, row: test_row)
      allow(UniqStore).to receive(:find_from_row).with(test_row).and_return(nil)
      allow(DataStore).to receive(:delete)
      allow(UniqStore).to receive(:upsert).with(new_id: 2, row: test_row)
    end

    it 'requests that the StateMap increment the current id' do
      subject
      expect(StateMap).to have_received(:increment_current_id)
    end

    it 'requests DataStore to recreate a new record with the id obtained from the StateMap' do
      subject
      expect(DataStore).to have_received(:create_new_record_from_row).with(id: 2, row: test_row)
    end

    it 'checks whether UniqStore already has a record' do
      subject
      expect(UniqStore).to have_received(:find_from_row).with(test_row)
    end

    context 'if a violating record is not found' do
      it 'will not request any deletions' do
        expect(DataStore).not_to have_received(:delete)
      end
    end

    context 'if a violating record is found in UniqStore' do
      before do
        allow(UniqStore).to receive(:find_from_row).and_return(1)
        allow(DataStore).to receive(:delete).with(1)
      end

      it 'requests that DataStore delete that record' do
        subject
        expect(DataStore).to have_received(:delete).with(1)
      end
    end

    it 'creates upserts a UniqStore record with current_id' do
      subject
      expect(UniqStore).to have_received(:upsert).with(row: test_row, new_id: 2)
    end
  end
end
