/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/ColumnPrinter.hh"

#include "orc/Exceptions.hh"

#include <getopt.h>
#include <string>
#include <memory>
#include <iostream>
#include <string>
#include <sys/time.h>
#include <fstream>
#include <unistd.h>

const int PRED = 40000;
void scanFile(std::ostream &out, const char *filename, uint64_t batchSize,
              const orc::RowReaderOptions &rowReaderOpts) {
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader =
            orc::createReader(orc::readFile(filename), readerOpts);
    std::cout << "compression for file " << filename << ": " << compressionKindToString(reader->getCompression()) << std::endl;
    std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);
    std::cout << "compression size for reader " << filename << ": " <<   reader->getCompressionSize() << std::endl;
    std::unique_ptr<orc::ColumnVectorBatch> batch =
            rowReader->createRowBatch(batchSize);

    unsigned long rows = 0;
    unsigned long batches = 0;
    unsigned long qualified = 0;
    int64_t value;
//    std::string line;
//    std::unique_ptr<orc::ColumnPrinter> printer =
//            createColumnPrinter(line, &rowReader->getSelectedType());

    while (rowReader->next(*batch)) {
//        printer->reset(*batch);
        orc::StructVectorBatch* structBatch  = dynamic_cast<orc::StructVectorBatch*>( batch.get() );
        orc::LongVectorBatch* longVector = dynamic_cast<orc::LongVectorBatch*>(structBatch->fields[0]);
        batches += 1;
        rows += batch->numElements;
        for (uint64_t r = 0; r < batch->numElements; ++r) {
            value = longVector->data[r];
            if (batch->notNull[r]){
                if (value > PRED) {
                    qualified++;
                }
            }
        }
    }
    out << "Rows: " << rows << std::endl;
    out << "Batches: " << batches << std::endl;
    std::cout << "number of value grater than " << PRED << ": " << qualified << std::endl;
}

void SumFile(std::ostream &out, const char *filename, uint64_t batchSize,
              const orc::RowReaderOptions &rowReaderOpts) {
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader =
            orc::createReader(orc::readFile(filename), readerOpts);
    std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);
    std::unique_ptr<orc::ColumnVectorBatch> batch =
            rowReader->createRowBatch(batchSize);

    unsigned long rows = 0;
    unsigned long batches = 0;
    int64_t value;
    int64_t sum = 0;

    while (rowReader->next(*batch)) {
//        printer->reset(*batch);
        orc::StructVectorBatch* structBatch  = dynamic_cast<orc::StructVectorBatch*>( batch.get() );
        orc::LongVectorBatch* longVector = dynamic_cast<orc::LongVectorBatch*>(structBatch->fields[0]);
        batches += 1;
        rows += batch->numElements;
        for (uint64_t r = 0; r < batch->numElements; ++r) {
            if (batch->notNull[r]){
                value = longVector->data[r];
                sum += value;
            }
        }
    }
    out << "Rows: " << rows << std::endl;
    out << "Batches: " << batches << std::endl;
    std::cout << "sum: " << sum << std::endl;
}

int main(int argc, char *argv[]) {
    static struct option longOptions[] = {
            {"help",    no_argument,       ORC_NULLPTR, 'h'},
            {"batch",   required_argument, ORC_NULLPTR, 'b'},
            {"columns", required_argument, ORC_NULLPTR, 'c'},
            {ORC_NULLPTR, 0,               ORC_NULLPTR, 0}
    };
    bool helpFlag = false;
    uint64_t batchSize = 1024;
    std::list<uint64_t> cols;
    orc::RowReaderOptions rowReaderOptions;
    int opt;
    char *tail;
    do {
        opt = getopt_long(argc, argv, "hb:c:", longOptions, ORC_NULLPTR);
        switch (opt) {
            case '?':
            case 'h':
                helpFlag = true;
                opt = -1;
                break;
            case 'b':
                batchSize = strtoul(optarg, &tail, 10);
                if (*tail != '\0') {
                    fprintf(stderr, "The --batch parameter requires an integer option.\n");
                    return 1;
                }
                break;
            case 'c': {
                char *col = std::strtok(optarg, ",");
                while (col) {
                    cols.push_back(static_cast<uint64_t>(std::atoi(col)));
                    col = std::strtok(ORC_NULLPTR, ",");
                }
                if (!cols.empty()) {
                    rowReaderOptions.include(cols);
                }
                break;
            }
            default:
                break;
        }
    } while (opt != -1);
    argc -= optind;
    argv += optind;

    if (argc < 1 || helpFlag) {
        std::cerr << "Usage: orc-scan [-h] [--help]\n"
                  << "                [-c 1,2,...] [--columns=1,2,...]\n"
                  << "                [-b<size>] [--batch=<size>] <filename>\n";
        return 1;
    } else {
        for (int i = 0; i < argc; ++i) {
            try {

//                sync();
//                std::ofstream ofs("/proc/sys/vm/drop_caches");
//                ofs << "3" << std::endl;

                struct timeval t_start, t_end;
                gettimeofday(&t_start, ORC_NULLPTR);
                clock_t c_start = clock();

                scanFile(std::cout, argv[i], batchSize, rowReaderOptions);


                clock_t totalCPUTime = (clock() - c_start);
                gettimeofday(&t_end, ORC_NULLPTR);
                double totalElapsedTime = (static_cast<double>(t_end.tv_sec - t_start.tv_sec) * 1000000.0
                                           + static_cast<double>(t_end.tv_usec - t_start.tv_usec)) / 1000000.0;
                std::cout << "filter,"<< argv[i] << "," <<totalElapsedTime <<
                          "," << static_cast<double>(totalCPUTime) / CLOCKS_PER_SEC<< std::endl;
                double tf= totalElapsedTime;
//                sync();
//                ofs << "3" << std::endl;

                gettimeofday(&t_start, ORC_NULLPTR);
                c_start = clock();

                SumFile(std::cout, argv[i], batchSize, rowReaderOptions);


                totalCPUTime = (clock() - c_start);
                gettimeofday(&t_end, ORC_NULLPTR);
                totalElapsedTime = (static_cast<double>(t_end.tv_sec - t_start.tv_sec) * 1000000.0
                                           + static_cast<double>(t_end.tv_usec - t_start.tv_usec)) / 1000000.0;
                double ts = totalElapsedTime;
                std::cout << "sum,"<< argv[i] << "," <<totalElapsedTime <<
                          "," << static_cast<double>(totalCPUTime) / CLOCKS_PER_SEC<< std::endl;

                std::cout << "filter,sum,"<< argv[i] << "," <<tf*1000 <<
                          "," << ts*1000<< std::endl;

            } catch (std::exception &ex) {
                std::cerr << "Caught exception in " << argv[i]
                          << ": " << ex.what() << "\n";
                return 1;
            }
        }
    }
    return 0;
}
