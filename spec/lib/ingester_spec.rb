require 'spec_helper'
require 'ingester'

describe Ingester do
  let(:valid_path) { './spec/support/ingester_acceptance_test.psv' }
  let(:valid_test_ingester) { Ingester.new(valid_path) }

  describe '.initialize' do
    it 'sets a new StateMap as @state_map' do
      expect(valid_test_ingester.state_map).to be_a(StateMap)
    end

    it 'sets the file path as @file_path' do
      expect(valid_test_ingester.file_path).to eq(valid_path)
    end

    it 'raises an error if given a misformatted PSV' do
      expect{ Ingester.new('./spec/support/malformed.psv') }.to raise_error(RuntimeError, 'The header row in ./spec/support/malformed.psv indicates an unexpected PSV format, aborting!')
    end
  end

  describe '#ingest' do
    context 'acceptance tests' do
      let(:state_map_connection)  { valid_test_ingester.state_map.connection }
      let(:data_store_connection) { DataStore.new('./data/test/data_store_0.pstore').connection }
      let(:uniq_store_connection) { UniqStore.connection('./data/test/uniq_store_2014.pstore') }
      before { valid_test_ingester.ingest }

      context 'first time it is run on a file' do
        it 'creates the tables and indices as-expected' do
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
        let(:second_run_ingester) { Ingester.new(valid_path) }
        it 'does not create any new databases or indices' do
          second_run_ingester.ingest('./spec/support/ingester_acceptance_test.psv')
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
          second_run_ingester.ingest('./spec/support/ingester_acceptance_test.psv')
          expect(data_store_connection.keys).to eq([9, 10, 11, 12, 14, 15, 16])
        end

        it 'updates all of the uniq_store records' do
          second_run_ingester.ingest('./spec/support/ingester_acceptance_test.psv')
          expect(uniq_store_connection.keys.count).to eq(6)
          expect(uniq_store_connection.get("2014-04-02.stb3.dupe one")).to eq(14)
        end

        it 'overwrites the indices as-expected' do
          second_run_ingester.ingest('./spec/support/ingester_acceptance_test.psv')
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
