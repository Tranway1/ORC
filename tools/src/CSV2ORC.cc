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

#include "orc/Exceptions.hh"
#include "orc/OrcFile.hh"
#include "Timezone.hh"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <getopt.h>
#include <string>
#include <sys/time.h>
#include <time.h>
#include <sstream>


static char gDelimiter = '|';

// extract one column raw text from one line
std::string extractColumn(std::string s, uint64_t colIndex) {
    uint64_t col = 0;
    size_t start = 0;
    size_t end = s.find(gDelimiter);
    while (col < colIndex && end != std::string::npos) {
        start = end + 1;
        end = s.find(gDelimiter, start);
        ++col;
    }
    return col == colIndex ? s.substr(start, end - start) : "";
}


orc::WriterOptions setOrcComp(orc::WriterOptions & options, std::string comp, int comp_level) {
    if (comp.find("lz4") != std::string::npos){
        options.setCompression(orc::CompressionKind_LZ4);
    }
    else if(comp.find("uncompressed") != std::string::npos){
        options.setCompression(orc::CompressionKind_NONE);
    }
    else if(comp.find("lzo") != std::string::npos){
        options.setCompression(orc::CompressionKind_LZO);
    }
    else if(comp.find("snappy") != std::string::npos){
        options.setCompression(orc::CompressionKind_SNAPPY);
    }
    else if(comp.find("zlib") != std::string::npos){
        options.setCompression(orc::CompressionKind_ZLIB);
    }
    else if(comp.find("zstd") != std::string::npos){
        options.setCompression(orc::CompressionKind_ZSTD);
        if (comp_level > 5){
            options.setCompressionStrategy(orc::CompressionStrategy_COMPRESSION);
        }
        else{
            options.setCompressionStrategy(orc::CompressionStrategy_SPEED);
        }
    }
    else {
        throw std::runtime_error(comp + " is not supported by ORC yet.");
    }
    return options;
}


// trim from start (in place)
static inline void ltrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
}

// trim from end (in place)
static inline void rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), s.end());
}

// trim from both ends (in place)
static inline void trim(std::string &s) {
    ltrim(s);
    rtrim(s);
}

std::string getORCSchemaFromFile(std::string f_name) {

    std::ifstream ifile("/home/chunwei/arrow/cpp/examples/parquet/tpcds/orc/" + f_name + ".schema");
    std::cout << "schema file path: ~/arrow/cpp/examples/parquet/tpcds/orc/" + f_name + ".schema" << std::endl;

    std::string res;
    std::string line;
    while (std::getline(ifile, line)) {
        trim(line);
        res.append(line);
    }
//    std::cout << "schema:" + res << std::endl;
    return res;
}

std::string Get_ORC_File(std::string f_name, std::string comp) {
    return f_name + "_" + comp + ".orc";
}


std::string GetFileName(std::string my_str) {
    std::vector<std::string> result;
    std::stringstream s_stream(my_str); //create string stream from the string
    while (s_stream.good()) {
        std::string substr;
        getline(s_stream, substr, '/');
        result.push_back(substr);
    }
    return result.at(result.size() - 1);
}


void fillLongValues(const std::vector<std::string> &data,
                    orc::ColumnVectorBatch *batch,
                    uint64_t numValues,
                    uint64_t colIndex) {
    orc::LongVectorBatch *longBatch =
            dynamic_cast<orc::LongVectorBatch *>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        std::string col = extractColumn(data[i], colIndex);
        if (col.empty()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            longBatch->data[i] = atoll(col.c_str());
        }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
}

void fillStringValues(const std::vector<std::string> &data,
                      orc::ColumnVectorBatch *batch,
                      uint64_t numValues,
                      uint64_t colIndex,
                      orc::DataBuffer<char> &buffer,
                      uint64_t &offset) {
    orc::StringVectorBatch *stringBatch =
            dynamic_cast<orc::StringVectorBatch *>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        std::string col = extractColumn(data[i], colIndex);
        if (col.empty()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            char *oldBufferAddress = buffer.data();
            // Resize the buffer in case buffer does not have remaining space to store the next string.
            while (buffer.size() - offset < col.size()) {
                buffer.resize(buffer.size() * 2);
            }
            char *newBufferAddress = buffer.data();
            // Refill stringBatch->data with the new addresses, if buffer's address has changed.
            if (newBufferAddress != oldBufferAddress) {
                for (uint64_t refillIndex = 0; refillIndex < i; ++refillIndex) {
                    stringBatch->data[refillIndex] =
                            stringBatch->data[refillIndex] - oldBufferAddress + newBufferAddress;
                }
            }
            memcpy(buffer.data() + offset,
                   col.c_str(),
                   col.size());
            stringBatch->data[i] = buffer.data() + offset;
            stringBatch->length[i] = static_cast<int64_t>(col.size());
            offset += col.size();
        }
    }
    stringBatch->hasNulls = hasNull;
    stringBatch->numElements = numValues;
}

void fillDoubleValues(const std::vector<std::string> &data,
                      orc::ColumnVectorBatch *batch,
                      uint64_t numValues,
                      uint64_t colIndex) {
    orc::DoubleVectorBatch *dblBatch =
            dynamic_cast<orc::DoubleVectorBatch *>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        std::string col = extractColumn(data[i], colIndex);
        if (col.empty()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            dblBatch->data[i] = atof(col.c_str());
        }
    }
    dblBatch->hasNulls = hasNull;
    dblBatch->numElements = numValues;
}

// parse fixed point decimal numbers
void fillDecimalValues(const std::vector<std::string> &data,
                       orc::ColumnVectorBatch *batch,
                       uint64_t numValues,
                       uint64_t colIndex,
                       size_t scale,
                       size_t precision) {


    orc::Decimal128VectorBatch *d128Batch = ORC_NULLPTR;
    orc::Decimal64VectorBatch *d64Batch = ORC_NULLPTR;
    if (precision <= 18) {
        d64Batch = dynamic_cast<orc::Decimal64VectorBatch *>(batch);
        d64Batch->scale = static_cast<int32_t>(scale);
    } else {
        d128Batch = dynamic_cast<orc::Decimal128VectorBatch *>(batch);
        d128Batch->scale = static_cast<int32_t>(scale);
    }
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        std::string col = extractColumn(data[i], colIndex);
        if (col.empty()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            size_t ptPos = col.find('.');
            size_t curScale = 0;
            std::string num = col;
            if (ptPos != std::string::npos) {
                curScale = col.length() - ptPos - 1;
                num = col.substr(0, ptPos) + col.substr(ptPos + 1);
            }
            orc::Int128 decimal(num);
            while (curScale != scale) {
                curScale++;
                decimal *= 10;
            }
            if (precision <= 18) {
                d64Batch->values[i] = decimal.toLong();
            } else {
                d128Batch->values[i] = decimal;
            }
        }
    }
    batch->hasNulls = hasNull;
    batch->numElements = numValues;
}

void fillBoolValues(const std::vector<std::string> &data,
                    orc::ColumnVectorBatch *batch,
                    uint64_t numValues,
                    uint64_t colIndex) {
    orc::LongVectorBatch *boolBatch =
            dynamic_cast<orc::LongVectorBatch *>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        std::string col = extractColumn(data[i], colIndex);
        if (col.empty()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            std::transform(col.begin(), col.end(), col.begin(), ::tolower);
            if (col == "true" || col == "t") {
                boolBatch->data[i] = true;
            } else {
                boolBatch->data[i] = false;
            }
        }
    }
    boolBatch->hasNulls = hasNull;
    boolBatch->numElements = numValues;
}

// parse date string from format YYYY-mm-dd
void fillDateValues(const std::vector<std::string> &data,
                    orc::ColumnVectorBatch *batch,
                    uint64_t numValues,
                    uint64_t colIndex) {
    orc::LongVectorBatch *longBatch =
            dynamic_cast<orc::LongVectorBatch *>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        std::string col = extractColumn(data[i], colIndex);
        if (col.empty()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            struct tm tm;
            memset(&tm, 0, sizeof(struct tm));
            strptime(col.c_str(), "%Y-%m-%d", &tm);
            time_t t = mktime(&tm);
            time_t t1970 = 0;
            double seconds = difftime(t, t1970);
            int64_t days = static_cast<int64_t>(seconds / (60 * 60 * 24));
            longBatch->data[i] = days;
        }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
}

// parse timestamp values in seconds
void fillTimestampValues(const std::vector<std::string> &data,
                         orc::ColumnVectorBatch *batch,
                         uint64_t numValues,
                         uint64_t colIndex) {
    struct tm timeStruct;
    orc::TimestampVectorBatch *tsBatch =
            dynamic_cast<orc::TimestampVectorBatch *>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        std::string col = extractColumn(data[i], colIndex);
        if (col.empty()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            memset(&timeStruct, 0, sizeof(timeStruct));
            char *left = strptime(col.c_str(), "%Y-%m-%d %H:%M:%S", &timeStruct);
            if (left == ORC_NULLPTR) {
                batch->notNull[i] = 0;
            } else {
                batch->notNull[i] = 1;
                tsBatch->data[i] = timegm(&timeStruct);
                char *tail;
                double d = strtod(left, &tail);
                if (tail != left) {
                    tsBatch->nanoseconds[i] = static_cast<long>(d * 1000000000.0);
                } else {
                    tsBatch->nanoseconds[i] = 0;
                }
            }
        }
    }
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = numValues;
}

static const char *GetDate(void) {
    static char buf[200];
    time_t t = time(ORC_NULLPTR);
    struct tm *p = localtime(&t);
    strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", p);
    return buf;
}

void WriteORCFile(std::string f_name, std::string comp, int comp_level = std::numeric_limits<int>::min()) {
    std::string p_name = Get_ORC_File(f_name, comp);
    std::cout << p_name << std::endl;
    double totalElapsedTime = 0.0;
    clock_t totalCPUTime = 0;
    uint64_t batchSize = 1024;

    ORC_UNIQUE_PTR<orc::OutputStream> outStream =
            orc::writeLocalFile(p_name);
    ORC_UNIQUE_PTR<orc::Type> schema(
            orc::Type::buildTypeFromString(getORCSchemaFromFile(GetFileName(f_name))));
    orc::WriterOptions options;
    options = setOrcComp(options, comp, comp_level);
    ORC_UNIQUE_PTR<orc::Writer> writer =
            createWriter(*schema, outStream.get(), options);
    ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch =
            writer->createRowBatch(batchSize);

    std::ifstream finput(f_name + ".dat");

    orc::DataBuffer<char> buffer(*orc::getDefaultPool(), 4 * 1024 * 1024);
    bool eof = false;
    std::string line;
    std::vector<std::string> data; // buffer that holds a batch of rows in raw text

    while (!eof) {
        uint64_t numValues = 0;      // num of lines read in a batch
        uint64_t bufferOffset = 0;   // current offset in the string buffer

        data.clear();
        memset(rowBatch->notNull.data(), 1, batchSize);

        // read a batch of lines from the input file
        for (uint64_t i = 0; i < batchSize; ++i) {
            if (!std::getline(finput, line)) {
                eof = true;
                break;
            }
            data.push_back(line);
            ++numValues;
        }

        if (numValues != 0) {
            orc::StructVectorBatch *structBatch =
                    dynamic_cast<orc::StructVectorBatch *>(rowBatch.get());
            structBatch->numElements = numValues;

            for (uint64_t i = 0; i < structBatch->fields.size(); ++i) {
                const orc::Type *subType = schema->getSubtype(i);
                switch (subType->getKind()) {
                    case orc::BYTE:
                    case orc::INT:
                    case orc::SHORT:
                    case orc::LONG:
                        fillLongValues(data,
                                       structBatch->fields[i],
                                       numValues,
                                       i);
                        break;
                    case orc::STRING:
                    case orc::CHAR:
                    case orc::VARCHAR:
                    case orc::BINARY:
                        fillStringValues(data,
                                         structBatch->fields[i],
                                         numValues,
                                         i,
                                         buffer,
                                         bufferOffset);
                        break;
                    case orc::FLOAT:
                    case orc::DOUBLE:
                        fillDoubleValues(data,
                                         structBatch->fields[i],
                                         numValues,
                                         i);
                        break;
                    case orc::DECIMAL:
                        fillDecimalValues(data,
                                          structBatch->fields[i],
                                          numValues,
                                          i,
                                          subType->getScale(),
                                          subType->getPrecision());
                        break;
                    case orc::BOOLEAN:
                        fillBoolValues(data,
                                       structBatch->fields[i],
                                       numValues,
                                       i);
                        break;
                    case orc::DATE:
                        fillDateValues(data,
                                       structBatch->fields[i],
                                       numValues,
                                       i);
                        break;
                    case orc::TIMESTAMP:
                    case orc::TIMESTAMP_INSTANT:
                        fillTimestampValues(data,
                                            structBatch->fields[i],
                                            numValues,
                                            i);
                        break;
                    case orc::STRUCT:
                    case orc::LIST:
                    case orc::MAP:
                    case orc::UNION:
                        throw std::runtime_error(subType->toString() + " is not supported yet.");
                }
            }

            struct timeval t_start, t_end;
            gettimeofday(&t_start, ORC_NULLPTR);
            clock_t c_start = clock();

            writer->add(*rowBatch);

            totalCPUTime += (clock() - c_start);
            gettimeofday(&t_end, ORC_NULLPTR);
            totalElapsedTime +=
                    (static_cast<double>(t_end.tv_sec - t_start.tv_sec) * 1000000.0
                     + static_cast<double>(t_end.tv_usec - t_start.tv_usec)) / 1000000.0;
        }
    }

    struct timeval t_start, t_end;
    gettimeofday(&t_start, ORC_NULLPTR);
    clock_t c_start = clock();

    writer->close();

    totalCPUTime += (clock() - c_start);
    gettimeofday(&t_end, ORC_NULLPTR);
    totalElapsedTime +=
            (static_cast<double>(t_end.tv_sec - t_start.tv_sec) * 1000000.0
             + static_cast<double>(t_end.tv_usec - t_start.tv_usec)) / 1000000.0;

    std::cout << GetDate() << " Finish importing Orc file." << std::endl;
    std::cout << GetDate() << " Total writer elasped time: "
              << totalElapsedTime << "s." << std::endl;
    std::cout << GetDate() << " Total writer CPU time: "
              << static_cast<double>(totalCPUTime) / CLOCKS_PER_SEC
              << "s." << std::endl;
    std::cout << "close input file." << std::endl;
//  os << parquet::EndRowGroup;
    std::cout << "close input file." << std::endl;
    std::cout << "Parquet Stream Writing complete." << std::endl;

}

void usage() {
    std::cout << "Usage: csv-import [-h] [--help]\n"
              << "                  [-d <character>] [--delimiter=<character>]\n"
              << "                  [-s <size>] [--stripe=<size>]\n"
              << "                  [-c <size>] [--block=<size>]\n"
              << "                  [-b <size>] [--batch=<size>]\n"
              << "                  <infile> <comp> <comp_level>\n"
              << "Import CSV file into an Orc file using the specified schema.\n"
              << "Compound types are not yet supported.\n";
}

int main(int argc, char *argv[]) {
    std::string ifile;
    std::string comp;
    int comp_level;
    uint64_t stripeSize = (128 << 20); // 128M
    uint64_t blockSize = 64 << 10;     // 64K
    uint64_t batchSize = 1024;


    static struct option longOptions[] = {
            {"help",      no_argument,       ORC_NULLPTR, 'h'},
            {"delimiter", required_argument, ORC_NULLPTR, 'd'},
            {"stripe",    required_argument, ORC_NULLPTR, 'p'},
            {"block",     required_argument, ORC_NULLPTR, 'c'},
            {"batch",     required_argument, ORC_NULLPTR, 'b'},
            {ORC_NULLPTR, 0,                 ORC_NULLPTR, 0}
    };
    bool helpFlag = false;
    int opt;
    char *tail;
    do {
        opt = getopt_long(argc, argv, "i:o:s:b:c:p:h", longOptions, ORC_NULLPTR);
        switch (opt) {
            case '?':
            case 'h':
                helpFlag = true;
                opt = -1;
                break;
            case 'd':
                gDelimiter = optarg[0];
                break;
            case 'p':
                stripeSize = strtoul(optarg, &tail, 10);
                if (*tail != '\0') {
                    fprintf(stderr, "The --stripe parameter requires an integer option.\n");
                    return 1;
                }
                break;
            case 'c':
                blockSize = strtoul(optarg, &tail, 10);
                if (*tail != '\0') {
                    fprintf(stderr, "The --block parameter requires an integer option.\n");
                    return 1;
                }
                break;
            case 'b':
                batchSize = strtoul(optarg, &tail, 10);
                if (*tail != '\0') {
                    fprintf(stderr, "The --batch parameter requires an integer option.\n");
                    return 1;
                }
                break;
        }
    } while (opt != -1);

    argc -= optind;
    argv += optind;

    if (argc != 3 || helpFlag) {
        usage();
        return 1;
    }

    ifile = argv[0];
    comp = argv[1];
    comp_level = std::stoi(argv[2]);

    std::string orc_file = Get_ORC_File(ifile, comp);
    std::cout << ifile << " -> "<<orc_file << std::endl;
    std::cout << GetDate() << " Start importing Orc file..." << std::endl;
    ORC_UNIQUE_PTR<orc::Type> fileType = orc::Type::buildTypeFromString(getORCSchemaFromFile(GetFileName(ifile)));

    double totalElapsedTime = 0.0;
    clock_t totalCPUTime = 0;

    orc::DataBuffer<char> buffer(*orc::getDefaultPool(), 4 * 1024 * 1024);

    orc::WriterOptions options;
    options.setStripeSize(stripeSize);
    options.setCompressionBlockSize(blockSize);
    options = setOrcComp(options, comp, comp_level);

    ORC_UNIQUE_PTR<orc::OutputStream> outStream = orc::writeLocalFile(orc_file);
    ORC_UNIQUE_PTR<orc::Writer> writer =
            orc::createWriter(*fileType, outStream.get(), options);
    ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch =
            writer->createRowBatch(batchSize);

    bool eof = false;
    std::string line;
    std::vector<std::string> data; // buffer that holds a batch of rows in raw text
    std::ifstream finput(ifile + ".dat");
    std::cout << GetDate() << " input file:" + ifile + ".dat" << std::endl;
    while (!eof) {
        uint64_t numValues = 0;      // num of lines read in a batch
        uint64_t bufferOffset = 0;   // current offset in the string buffer

        data.clear();
        memset(rowBatch->notNull.data(), 1, batchSize);

        // read a batch of lines from the input file
        for (uint64_t i = 0; i < batchSize; ++i) {
            if (!std::getline(finput, line)) {
                eof = true;
                break;
            }
            data.push_back(line);
            ++numValues;
        }

        if (numValues != 0) {
            orc::StructVectorBatch *structBatch =
                    dynamic_cast<orc::StructVectorBatch *>(rowBatch.get());
            structBatch->numElements = numValues;

            for (uint64_t i = 0; i < structBatch->fields.size(); ++i) {
                const orc::Type *subType = fileType->getSubtype(i);
                switch (subType->getKind()) {
                    case orc::BYTE:
                    case orc::INT:
                    case orc::SHORT:
                    case orc::LONG:
                        fillLongValues(data,
                                       structBatch->fields[i],
                                       numValues,
                                       i);
                        break;
                    case orc::STRING:
                    case orc::CHAR:
                    case orc::VARCHAR:
                    case orc::BINARY:
                        fillStringValues(data,
                                         structBatch->fields[i],
                                         numValues,
                                         i,
                                         buffer,
                                         bufferOffset);
                        break;
                    case orc::FLOAT:
                    case orc::DOUBLE:
                        fillDoubleValues(data,
                                         structBatch->fields[i],
                                         numValues,
                                         i);
                        break;
                    case orc::DECIMAL:
                        fillDecimalValues(data,
                                          structBatch->fields[i],
                                          numValues,
                                          i,
                                          subType->getScale(),
                                          subType->getPrecision());
                        break;
                    case orc::BOOLEAN:
                        fillBoolValues(data,
                                       structBatch->fields[i],
                                       numValues,
                                       i);
                        break;
                    case orc::DATE:
                        fillDateValues(data,
                                       structBatch->fields[i],
                                       numValues,
                                       i);
                        break;
                    case orc::TIMESTAMP:
                    case orc::TIMESTAMP_INSTANT:
                        fillTimestampValues(data,
                                            structBatch->fields[i],
                                            numValues,
                                            i);
                        break;
                    case orc::STRUCT:
                    case orc::LIST:
                    case orc::MAP:
                    case orc::UNION:
                        throw std::runtime_error(subType->toString() + " is not supported yet.");
                }
            }

            struct timeval t_start, t_end;
            gettimeofday(&t_start, ORC_NULLPTR);
            clock_t c_start = clock();

            writer->add(*rowBatch);

            totalCPUTime += (clock() - c_start);
            gettimeofday(&t_end, ORC_NULLPTR);
            totalElapsedTime +=
                    (static_cast<double>(t_end.tv_sec - t_start.tv_sec) * 1000000.0
                     + static_cast<double>(t_end.tv_usec - t_start.tv_usec)) / 1000000.0;
        }
    }

    struct timeval t_start, t_end;
    gettimeofday(&t_start, ORC_NULLPTR);
    clock_t c_start = clock();

    writer->close();

    totalCPUTime += (clock() - c_start);
    gettimeofday(&t_end, ORC_NULLPTR);
    totalElapsedTime +=
            (static_cast<double>(t_end.tv_sec - t_start.tv_sec) * 1000000.0
             + static_cast<double>(t_end.tv_usec - t_start.tv_usec)) / 1000000.0;

    std::cout << GetDate() << " Finish importing Orc file." << std::endl;
    std::cout << GetDate() << " Total writer elasped time: "
              << totalElapsedTime << "s." << std::endl;
    std::cout << GetDate() << " Total writer CPU time: "
              << static_cast<double>(totalCPUTime) / CLOCKS_PER_SEC
              << "s." << std::endl;

    std::ifstream in_file(orc_file, std::ios::binary);
    in_file.seekg(0, std::ios::end);
    auto file_size = in_file.tellg();

    std::cout << comp <<","<< comp_level <<"," << orc_file << "," <<
        file_size<< "," <<totalElapsedTime <<
        "," << static_cast<double>(totalCPUTime) / CLOCKS_PER_SEC<< std::endl;
    return 0;
}
