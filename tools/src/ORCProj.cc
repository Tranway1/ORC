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
#include "../../c++/test/MemoryOutputStream.hh"

#include <getopt.h>
#include <string>
#include <memory>
#include <iostream>
#include <string>
#include <sys/time.h>
#include <fstream>
#include <unistd.h>
#include <algorithm>
#include <unordered_map>


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

void ScanORCWithIndices(std::string f_name, int* array, int size){
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  uint64_t batchSize = 1024;
  std::list<uint64_t> cols;
  orc::RowReaderOptions rowReaderOptions;
  cols.push_back(1);
  rowReaderOptions.include(cols);

  orc::ReaderOptions readerOpts;
  std::unique_ptr<orc::Reader> reader =
          orc::createReader(orc::readFile(f_name), readerOpts);
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOptions);
  std::unique_ptr<orc::ColumnVectorBatch> batch =
          rowReader->createRowBatch(batchSize);


  int32_t* res_array = new int32_t[size];

  int64_t value = 0;
  uint64_t row_cnt = 0;
  int idx = 0;
  unsigned long loc = static_cast<unsigned long>(array[idx]);

  while (rowReader->next(*batch)) {
//        printer->reset(*batch);
    orc::StructVectorBatch* structBatch  = dynamic_cast<orc::StructVectorBatch*>( batch.get() );
    orc::LongVectorBatch* longVector = dynamic_cast<orc::LongVectorBatch*>(structBatch->fields[0]);


    uint64_t cur_rows=batch->numElements;
//    std::cout << "\t\t---# rows of current data chunk: "<< cur_rows<< std::endl;

    if (row_cnt + cur_rows < loc + 1){
      row_cnt+=cur_rows;
      continue;
    }

    for (uint64_t r = 0; r < batch->numElements; ++r) {
        value = longVector->data[r];

    }

    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      value = longVector->data[(loc-row_cnt)];
      res_array[idx]=(int)value;
      idx++;
      if (idx==size){
        break;
      }
      loc = static_cast<unsigned long>(array[idx]);
    }

    if (idx==size){
      break;
    }
    row_cnt+=cur_rows;
  }

  std::cout << "res length: " << idx << std::endl;
  std::chrono::steady_clock::time_point lend = std::chrono::steady_clock::now();
  auto lookup_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(lend - begin).count();
  std::cout << "time elapsed in lookup ORC: "<< lookup_arrow << std::endl;
//
//  for (int i=0;i<size;i++){
//    std::cout << "value lookup for row "<<array[i]<<": "<< res_array[i] << std::endl;
//  }
}

void randperm(int *matrix, int size) {
  std::random_shuffle(matrix, matrix + size);
}


int *genArray0ton(int n) {
  int *arr = new int[n];
  int i = 0;
  for (i = 0; i < n; i++) {
    arr[i] = i;
  }
  return arr;
}



int *getRandomIdx(int n, float r) {
  int num = static_cast<int>(static_cast<float>(n) * r);
  int *arr = genArray0ton(n);
  randperm(arr, n);
  int *res = new int[num];
  for (int i = 0; i < num; i++) {
    res[i] = arr[i];
//    std::cout << res[i] << " ";
  }
  std::sort(res, res + num);
  std::cout << "res length" << num << std::endl;
//  for (int i = 0; i < num; ++i)
//    std::cout << res[i] << " ";
  return res;
}

std::string Get_ORC_File(std::string f_name, std::string comp) {
  return f_name + "_" + comp + ".orc";
}


int main(int argc, char *argv[]) {
  std::cout << "You have entered " << argc
            << " arguments:" << "\n";

  bool clearcache = false;
  for (int i = 0; i < argc; ++i)
    std::cout << argv[i] << "\n";

  std::string f_name = argv[1];
  std::string comp = argv[2];
  float ratio = std::stof(argv[3]);
  int num=14401261;
//  int num=1441548;
  std::cout << "lookup with ratio "<<ratio<<" on "<<f_name<<" with " <<comp << "\n";

  int *indices = getRandomIdx(num,ratio);

  std::ofstream ofs("/proc/sys/vm/drop_caches");
  if (clearcache){
    sync();
    std::cout << "Clear cache ... "<<std::endl;
    ofs << "3" << std::endl;
  }

  std::cout << "----Lookup parquet" << std::endl;
  auto begin = std::chrono::steady_clock::now();
  ScanORCWithIndices(Get_ORC_File(f_name,comp), indices, static_cast<int>(static_cast<float>(num) * ratio));
  auto end = std::chrono::steady_clock::now();
  auto orc_proj = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "Query run time orc: " << orc_proj<<std::endl;

  return 0;
}
