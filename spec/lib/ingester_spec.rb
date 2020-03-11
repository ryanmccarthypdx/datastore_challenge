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

  describe '.ingest' do
    context 'unit tests' do
      it 'raises an error if it tries to ingest a misformatted PSV' do
        expect{ Ingester.ingest('./spec/support/malformed.psv') }.to raise_error(RuntimeError, 'The header row in ./spec/support/malformed.psv indicates an unexpected PSV format, aborting!')
      end
    end

    context 'acceptance tests' do
      let(:state_map_connection)  { StateMap.connection }
      let(:data_store_connection) { DataStore.connection('./data/test/data_store_0.pstore') }
      let(:uniq_store_connection) { UniqStore.connection('./data/test/uniq_store_2014.pstore') }
      before { Ingester.ingest('./spec/support/ingester_acceptance_test.psv') }

      context 'first time it is run on a file' do
        it 'creates the dbs and indices as-expected' do
          expect(Dir['./data/test/**/*.pstore']).to contain_exactly(
            "./data/test/uniq_store_1066.pstore",
            "./data/test/data_store_0.pstore",
            "./data/test/uniq_store_2014.pstore",
            "./data/test/state_map.pstore",
            "./data/test/indices/DATE.pstore",
            "./data/test/indices/PROVIDER.pstore",
            "./data/test/indices/REV.pstore",
            "./data/test/indices/STB.pstore",
            "./data/test/indices/TITLE.pstore",
            "./data/test/indices/VIEW_TIME.pstore")
        end

        it 'populates the state_map as-expected' do
          expect(state_map_connection.keys).to contain_exactly(:current_id, :data_store_id_ranges, :starting_id_of_current_data_store)
          expect(state_map_connection.get(:current_id)).to eq(8)
          expect(state_map_connection.get(:data_store_id_ranges)).to eq([])
          expect(state_map_connection.get(:starting_id_of_current_data_store)).to eq(0)
        end

        it 'populates the data_store as-expected' do
          expect(data_store_connection.keys).to eq([1, 2, 3, 4, 6, 7, 8]) # 5 is overwritten by 6
          expect(data_store_connection.get(1)).to eq(["stb1", "the matrix", "warner bros", "2014-04-01", "4.00", "1:30"])
        end

        it 'populates the uniq_store as-expected' do
          expect(uniq_store_connection.keys).to contain_exactly(
            "2014-04-01.stb1.the matrix",
            "2014-04-03.stb1.unbreakable",
            "2014-04-02.stb2.the hobbit",
            "2014-04-02.stb3.the matrix",
            "2014-04-02.stb3.dupe one",
            "2014-04-02.stb1.not a dupe"
          )
          expect(uniq_store_connection.get("2014-04-02.stb3.dupe one")).to eq(6) # and not 5
        end

        it 'populates the indices as-expected' do
          expect(Index.connections_for_ingest[PSV_HEADERS.index("TITLE")].keys)
            .to contain_exactly("the matrix",
                                "unbreakable",
                                "the hobbit",
                                "dupe one",
                                "not a dupe")
          expect(Index.connections_for_ingest[PSV_HEADERS.index("TITLE")].get("not a dupe")).to contain_exactly(7,8)
        end
      end

      context 'when run a second time' do
        it 'does not create any new databases or indices' do
          Ingester.ingest('./spec/support/ingester_acceptance_test.psv')
          expect(Dir['./data/test/**/*.pstore']).to contain_exactly(
            "./data/test/uniq_store_1066.pstore",
            "./data/test/data_store_0.pstore",
            "./data/test/uniq_store_2014.pstore",
            "./data/test/state_map.pstore",
            "./data/test/indices/DATE.pstore",
            "./data/test/indices/PROVIDER.pstore",
            "./data/test/indices/REV.pstore",
            "./data/test/indices/STB.pstore",
            "./data/test/indices/TITLE.pstore",
            "./data/test/indices/VIEW_TIME.pstore")
        end

        it 'overwrites all existing data_store records with new ones' do
          Ingester.ingest('./spec/support/ingester_acceptance_test.psv')
          expect(data_store_connection.keys).to eq([9, 10, 11, 12, 14, 15, 16])
        end

        it 'updates all of the uniq_store records' do
          Ingester.ingest('./spec/support/ingester_acceptance_test.psv')
          expect(uniq_store_connection.keys.count).to eq(6)
          expect(uniq_store_connection.get("2014-04-02.stb3.dupe one")).to eq(14)
        end

        it 'overwrites the indices as-expected' do
          Ingester.ingest('./spec/support/ingester_acceptance_test.psv')
          expect(Index.connections_for_ingest[PSV_HEADERS.index("TITLE")].keys)
            .to contain_exactly("the matrix",
                                "unbreakable",
                                "the hobbit",
                                "dupe one",
                                "not a dupe")
          expect(Index.connections_for_ingest[PSV_HEADERS.index("TITLE")].get("not a dupe")).to contain_exactly(15,16)
        end
      end
    end
  end
end
