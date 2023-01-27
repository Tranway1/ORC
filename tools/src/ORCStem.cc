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

const int PRED = 40000;

const uint64_t BATCHSIZE = 1024;

const int DEFAULT_MEM_STREAM_SIZE = 100 * 1024 * 1024; // 100M

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
//  std::cout << "schema:" + res << std::endl;
  return res;
}

std::string getORCFile(std::string f_name, std::string comp) {
  std::string orcfile = "/data/dataset/" + f_name + "_"+comp+".orc";
  return orcfile;
}





std::vector<int> UnionVector(std::vector<int>* projs,  std::vector<int>* filters){
  std::vector<int> un;
  std::set<int> set;
  for (int p: *projs){
    set.insert(p);
  }
  for (int f: *filters){
    set.insert(f);
  }
  std::set<int>::iterator it = set.begin();

  while (it != set.end()){
    un.push_back(*it);
    it++;
  }
  sort(un.begin(),un.end());
  return un;
}

bool IsProj (int x, std::vector<int>* projs){
  for (int p : *projs){
    if (p==x)
      return true;
  }
  return false;
}


inline bool doubleLess(double a, double b){
  return a<b;
}

inline bool doubleLE(double a, double b){
  return a<=b;
}

inline bool doubleEqual(double a, double b){
  return a==b;
  //  return fabs(a - b) < EPSILON;
}

inline bool doubleGreater(double a, double b){
  return a>b;
}

inline bool doubleGE(double a, double b){
  return a>=b;
}

inline bool intLess(int a, int b){
  return a<b;
}

inline bool intLE(int a, int b){
  return a<=b;
}

inline bool intEqual(int a, int b){
  return a==b;
}

inline bool intGreater(int a, int b){
  return a>b;
}

inline bool intGE(int a, int b){
  return a>=b;
}

inline bool strEqual(std::string a, std::string b){
  return a==b;
}

inline bool longLess(long a, long b){
  return a<b;
}

inline bool longLE(long a, long b){
  return a<=b;
}

inline bool longEqual(long a, long b){
  return a==b;
}

inline bool longGreater(long a, long b){
  return a>b;
}

inline bool longGE(long a, long b){
  return a>=b;
}



/// filter a double data chunk with given condition, output is list of qualified row number
/// \tparam T
/// \param chunk
/// \param idx_builder
/// \param pred: opand
/// \param func: predicate evaluation function
/// \param projected
/// \param pre
/// \param base
template<class T> void FilterDoubleChunk(orc::DoubleVectorBatch* doublechunk, std::vector<uint64_t>& idx_builder, double pred,
                       T func, std::shared_ptr<std::vector<uint64_t>> pre = nullptr, int64_t base = 0){

  double val = 0.0;
  if (pre != nullptr && pre->size()==0){
//    std::cout << "No qualified items and skipped! " <<std::endl;
    return ;
  }
  else if (pre == nullptr){
    uint64_t size = doublechunk->numElements;
    for (uint64_t idx = 0; idx < size; idx++){
      if (doublechunk->notNull[idx] == 1)
      {
        val = doublechunk->data[idx];
        if (func(val,pred)){
          idx_builder.push_back(idx);
        }
      }
    }
  }
  else{
    auto len = pre->size();
//    std::cout << "Number qualified items in previous: " << len <<std::endl;
    uint64_t loc = 0;
    for (uint64_t idx = 0; idx < len; idx++){
      loc = pre->at(idx);
      if (doublechunk->notNull[loc]==1)
      {
        val = doublechunk->data[loc];
        if (func(val,pred)){
          idx_builder.push_back(loc);
        }
      }
    }
  }
}


/// filter a int data chunk with given condition, output is list of qualified row number
/// \tparam T
/// \param chunk
/// \param idx_builder
/// \param pred: opand
/// \param func: predicate evaluation function
/// \param projected
/// \param pre
/// \param base
template<class T> void FilterLongChunk(orc::LongVectorBatch* longchunk, std::vector<uint64_t>& idx_builder, long pred,
                                         T func, std::shared_ptr<std::vector<uint64_t>> pre = nullptr, int64_t base = 0){

  long val = 0;
  if (pre != nullptr && pre->size()==0){
//    std::cout << "No qualified items and skipped! " <<std::endl;
    return ;
  }
  else if (pre == nullptr){
    uint64_t size = longchunk->numElements;
//    std::cout << " long vector chunk with size " << size <<std::endl;

    for (uint64_t idx = 0; idx < size; idx++){
//      if (longchunk->notNull[idx] == 1)
      {
        val = longchunk->data[idx];
        if (func(val,pred)){
          idx_builder.push_back(idx);
        }
      }
    }
  }
  else{
    auto len = pre->size();
//    std::cout << "Number qualified items in previous: " << len <<std::endl;
    uint64_t loc = 0;
    for (uint64_t idx = 0; idx < len; idx++){
      loc = pre->at(idx);
//      if (longchunk->notNull[loc]==1)
      {
        val = longchunk->data[loc];
        if (func(val,pred)){
          idx_builder.push_back(loc);
        }
      }
    }
  }
//  std::cout << "finish data vector with result size: "<< idx_builder.size() <<std::endl;
}


/// filter a string data chunk with given condition, output is list of qualified row number
/// \tparam T
/// \param chunk
/// \param idx_builder
/// \param pred: opand
/// \param func: predicate evaluation function
/// \param projected
/// \param pre
/// \param base
template<class T> void FilterStringChunk(orc::StringVectorBatch* stringchunk, std::vector<uint64_t>& idx_builder, std::string pred,
                                       T func, std::shared_ptr<std::vector<uint64_t>> pre = nullptr, int64_t base = 0){


  if (pre != nullptr && pre->size()==0){
//    std::cout << "No qualified items and skipped! " <<std::endl;
    return ;
  }
  else if (pre == nullptr){
    uint64_t size = stringchunk->numElements;
    for (uint64_t idx = 0; idx < size; idx++){
//      if (stringchunk->notNull[idx] == 1)
      {
        char value[stringchunk->length[idx]+1];
        memcpy(value,stringchunk->data[idx],static_cast<size_t>(stringchunk->length[idx]));
        value[stringchunk->length[idx]]='\0';
        if (func(value,pred)){
          idx_builder.push_back(idx);
        }
      }
    }
  }
  else{
    auto len = pre->size();
//    std::cout << "Number qualified items in previous: " << len <<std::endl;
    uint64_t loc = 0;
    for (uint64_t idx = 0; idx < len; idx++){
      loc = pre->at(idx);
//      if (stringchunk->notNull[loc]==1)
      {
        char value[stringchunk->length[loc]+1];
        memcpy(value,stringchunk->data[loc],static_cast<size_t>(stringchunk->length[loc]));
        value[stringchunk->length[loc]]='\0';
        if (func(value,pred)){
          idx_builder.push_back(loc);
        }
      }
    }
  }
}

/// Scan double data chunk based on the pre results
/// \param doublechunk
/// \param rvector
/// \param pre
/// \param base
void ScanDataChunk(orc::DoubleVectorBatch* doublechunk, orc::DoubleVectorBatch& rvector,
                   std::shared_ptr<std::vector<uint64_t>> pre = nullptr, uint64_t cnt = 0){

  double val = 0.0;
  bool hasNull = false;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    auto size =  doublechunk->numElements;
    for (uint64_t idx = 0; idx < size; idx++){
      if (doublechunk->notNull[idx]==1)
      {
        val = doublechunk->data[idx];
        rvector.notNull[cnt] = 1;
        rvector.data[cnt++] = val;
      }
      else{
        rvector.notNull[cnt++] = 0;
        hasNull = true;
      }
    }
  }
  else{
    auto len = pre->size();
    uint64_t loc = 0;
    for (uint64_t idx = 0; idx < len; idx++){
      loc = pre->at(idx);
//      std::cout << "Number of scan qualified items: " << len <<std::endl;
      if (doublechunk->notNull[loc]==1)
      {
        val = doublechunk->data[loc];
        rvector.notNull[cnt] = 1;
        rvector.data[cnt++] = val;
//        std::cout << loc<<"th value:" << val << "written into loc: "<< cnt <<std::endl;
      }
      else{
        rvector.notNull[cnt++] = 0;
        hasNull = true;
      }
    }
  }
  rvector.hasNulls = hasNull;
  rvector.numElements = cnt;
}


/// Scan long data chunk based on the pre results
/// \param doublechunk
/// \param rvector
/// \param pre
/// \param base
void ScanDataChunk(orc::LongVectorBatch* longchunk, orc::LongVectorBatch& rvector,
                   std::shared_ptr<std::vector<uint64_t>> pre = nullptr, uint64_t cnt = 0){

  long val = 0;
  bool hasNull = false;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    auto size =  longchunk->numElements;
    for (uint64_t idx = 0; idx < size; idx++){
      if (longchunk->notNull[idx]==1)
      {
        val = longchunk->data[idx];
        rvector.notNull[cnt] = 1;
        rvector.data[cnt++] = val;
      }
      else{
        rvector.notNull[cnt++] = 0;
        hasNull = true;
      }
    }
  }
  else{
    auto len = pre->size();
//    std::cout << "Number of scan qualified items: " << len <<std::endl;
    uint64_t loc = 0;
    for (uint64_t idx = 0; idx < len; idx++)
    {
      loc = pre->at(idx);
      if (longchunk->notNull[loc]==1)
      {
        val = longchunk->data[loc];
        rvector.notNull[cnt] = 1;
        rvector.data[cnt++] = val;
//        std::cout << loc<<"th value:" << val << "written into loc: "<< cnt <<std::endl;
      }
      else{
        rvector.notNull[cnt++] = 0;
        hasNull = true;
      }
    }
  }
  rvector.hasNulls = hasNull;
  rvector.numElements = cnt;
}



/// Scan string data chunk based on the pre results
/// \param doublechunk
/// \param rvector
/// \param pre
/// \param base
void ScanDataChunk(orc::StringVectorBatch* stringchunk, orc::StringVectorBatch& rvector,
                   std::shared_ptr<std::vector<uint64_t>> pre = nullptr, uint64_t cnt = 0){


  bool hasNull = false;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    auto size =  stringchunk->numElements;
    for (uint64_t idx = 0; idx < size; idx++){
      if (stringchunk->notNull[idx]==1)
      {
        char value[stringchunk->length[idx]+1];
        memcpy(value,stringchunk->data[idx],static_cast<size_t>(stringchunk->length[idx]));
        value[stringchunk->length[idx]]='\0';
        rvector.notNull[cnt] = 1;
        rvector.data[cnt++] = value;
      }
      else{
        rvector.notNull[cnt++] = 0;
        hasNull = true;
      }
    }
  }
  else{
    auto len = pre->size();
//    std::cout << "Number of scan qualified items: " << len <<std::endl;
    uint64_t loc = 0;
    for (uint64_t idx = 0; idx < len; idx++){
      loc = pre->at(idx);
      if (stringchunk->notNull[loc]==1)
      {
        char value[stringchunk->length[loc]+1];
        memcpy(value,stringchunk->data[loc],static_cast<size_t>(stringchunk->length[loc]));
        value[stringchunk->length[loc]]='\0';
        rvector.notNull[cnt] = 1;
        rvector.data[cnt++] = value;
      }
      else{
        rvector.notNull[cnt++] = 0;
        hasNull = true;
      }
    }
  }
  rvector.hasNulls = hasNull;
  rvector.numElements = cnt;
}

orc::StructVectorBatch * scanORCFile(std::basic_string<char> filename, const char *f_name, const char *query,
                                     uint64_t rsize, std::vector<int>* projs, std::vector<int>* filters,
                                     std::vector<std::string>* ops, std::vector<std::string>* opands) {
  ORC_UNIQUE_PTR<orc::Type> schema(
          orc::Type::buildTypeFromString(getORCSchemaFromFile(f_name)));

  ORC_UNIQUE_PTR<orc::Type> resultSchema(
          orc::Type::buildTypeFromString(getORCSchemaFromFile(query)));
  ORC_UNIQUE_PTR<orc::OutputStream> outStream =
          orc::writeLocalFile("/mnt/tmp/results.orc");
  orc::MemoryPool * pool = orc::getDefaultPool();
  orc::RowReaderOptions rowReaderOpts;


  orc::WriterOptions options;
  options.setCompression(orc::CompressionKind_NONE);
  options.setMemoryPool(pool);
  std::unique_ptr<orc::Writer> writer = createWriter(*resultSchema, outStream.get(), options);
  std::shared_ptr<orc::ColumnVectorBatch> res_batch =
          writer->createRowBatch(rsize);

  orc::StructVectorBatch * restructBatch =
          dynamic_cast<orc::StructVectorBatch *>(res_batch.get());
//  orc::StringVectorBatch * strBatch =
//          dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[0]);

  auto cols = UnionVector(projs, filters);

  std::list<uint64_t> includeCols;
  for (int col: cols){
    includeCols.push_back(static_cast<unsigned long>(col));
  }

  std::unordered_map<int, int> res_map;

  for (unsigned long i=0;i<projs->size();i++){
    res_map[projs->at((i))]=(int)i;
    std::cout << projs->at((i)) << "---" << i << std::endl;
  }

  std::unordered_map<int, int> col_map;
  int count = 0;
  for (int col: cols){
    col_map[col]=count;
    std::cout <<col<< "---" << count<< std::endl;
    count++;
  }
  rowReaderOpts.include(includeCols);

  auto begin = std::chrono::steady_clock::now();

  orc::ReaderOptions readerOpts;
  std::unique_ptr<orc::Reader> reader =
          orc::createReader(orc::readFile(filename), readerOpts);
  std::cout << "compression for file " << filename << ": " << compressionKindToString(reader->getCompression()) << std::endl;
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);
  std::cout << "compression size for reader " << filename << ": " <<   reader->getCompressionSize() << std::endl;
  std::unique_ptr<orc::ColumnVectorBatch> batch =
          rowReader->createRowBatch(BATCHSIZE);

  auto end = std::chrono::steady_clock::now();
  auto time_read = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time for raed file: " << time_read<<std::endl;


  unsigned long rows = 0;
  unsigned long batches = 0;

  long time_compute = 0;
  long time_load = 0;

  auto cbegin = std::chrono::steady_clock::now();
  int cnt = 0;
  while (rowReader->next(*batch)){
      auto cend = std::chrono::steady_clock::now();
      time_load += std::chrono::duration_cast<std::chrono::microseconds>(cend - cbegin).count();

    orc::StructVectorBatch* structBatch  = dynamic_cast<orc::StructVectorBatch*>( batch.get() );
    std::shared_ptr<std::vector<uint64_t>> pre = nullptr;


    //start filters
    for (unsigned long findex = 0; findex < filters->size();findex++){
      auto col_idx = filters->at(findex);
//      std::cout << "index of map: " << col_map[col_idx] << std::endl;
      std::vector<uint64_t> indices;
      if (findex>0){
//        std::cout << "next filter with pre size " << pre->size() << std::endl;
      }

      std::string predicate;
      auto attr = schema->getSubtype(static_cast<uint64_t>(col_idx));
//      std::cout << "column type: " << attr->getKind() << std::endl;
      auto coldata = structBatch->fields[static_cast<unsigned long>(col_map[col_idx])];

      switch (attr->getKind()) {
        case orc::FLOAT:
        case orc::DOUBLE:
        {
          orc::DoubleVectorBatch* doubleVector = dynamic_cast<orc::DoubleVectorBatch*>(coldata);
          predicate = opands->at(findex);
          if (ops->at(findex) == "EQUAL"){
            FilterDoubleChunk(doubleVector, indices, std::stod(predicate),
                              doubleEqual, pre);
          }
          else if (ops->at(findex) == "GREATER"){
            FilterDoubleChunk(doubleVector, indices, std::stod(predicate),
                              doubleGreater, pre);
          }
          else if (ops->at(findex) == "GREATER_EQUAL"){
            FilterDoubleChunk(doubleVector, indices, std::stod(predicate),
                              doubleGE, pre);
          }
          else if (ops->at(findex) == "LESS"){
            FilterDoubleChunk(doubleVector, indices, std::stod(predicate),
                              doubleLess, pre);
          }
          else if (ops->at(findex) == "LESS_EQUAL"){
            FilterDoubleChunk(doubleVector, indices, std::stod(predicate),
                              doubleLE, pre);
          }
          break;
        }
        case orc::BYTE:
        case orc::INT:
        case orc::SHORT:
        case orc::LONG:
        {
          predicate = opands->at(findex);
          orc::LongVectorBatch* longVector = dynamic_cast<orc::LongVectorBatch*>(coldata);
//          std::cout << attr->getKind() << " column type with operator " << ops->at(findex) <<std::endl;
          if (ops->at(findex) == "EQUAL")
            FilterLongChunk(longVector, indices, std::stol(predicate),
                              longEqual, pre);
          else if (ops->at(findex) == "GREATER")
            FilterLongChunk(longVector, indices, std::stol(predicate),
                            longGreater, pre);
          else if (ops->at(findex) == "GREATER_EQUAL")
            FilterLongChunk(longVector, indices, std::stol(predicate),
                            longGE, pre);
          else if (ops->at(findex) == "LESS_EQUAL")
            FilterLongChunk(longVector, indices, std::stol(predicate),
                            longLE, pre);
          else if (ops->at(findex) == "LESS")
            FilterLongChunk(longVector, indices, std::stol(predicate),
                            longLess, pre);
          break;
        }
        case orc::STRING:
        case orc::CHAR:
        case orc::VARCHAR:
        case orc::BINARY:
        {
          predicate = opands->at(findex);
          orc::StringVectorBatch* stringVector = dynamic_cast<orc::StringVectorBatch*>(coldata);
//          std::cout<< "string oprand: "<< predicate<<std::endl;
          if (ops->at(findex) == "EQUAL")
            FilterStringChunk(stringVector, indices, predicate,
                            strEqual, pre);
          else
            throw std::runtime_error(ops->at(findex) + " operator is not supported for string yet.");
          break;
        }


        default:
          throw std::runtime_error(std::to_string(attr->getKind()) + " type is not supported yet.");

      }
      pre = std::make_shared<std::vector<uint64_t>>(indices);
//      std::cout << "finished filtering on " << col_idx <<  ": " << pre->size() << std::endl;
    }
    if (pre->size()==0){
      batches += 1;
      rows += batch->numElements;
      cbegin = std::chrono::steady_clock::now();
      continue;
    }
//    std::cout << "# before project: " << pre->size() << std::endl;

    //start projection
    for (unsigned long  pindex = 0; pindex < projs->size();pindex++){
      auto col_idx = projs->at(pindex);
      auto attr = schema->getSubtype(static_cast<uint64_t>(col_idx));
      auto coldata = structBatch->fields[static_cast<unsigned long>(col_map[col_idx])];
      auto colres = restructBatch->fields[static_cast<unsigned long>(res_map[col_idx])];
//      std::cout << attr->getKind() << resultSchema->getSubtype(static_cast<unsigned long>(res_map[col_idx]))->getKind() << std::endl;
      switch (attr->getKind()) {
        case orc::FLOAT:
        case orc::DOUBLE:
        {
          orc::DoubleVectorBatch* doubleVector = dynamic_cast<orc::DoubleVectorBatch*>(coldata);
          orc::DoubleVectorBatch* resVector = dynamic_cast<orc::DoubleVectorBatch*>(colres);
//          ScanDataChunk(orc::LongVectorBatch* longchunk, orc::LongVectorBatch& rvector,
//                             std::vector<uint64_t>* pre = nullptr, uint64_t cnt = 0)
          if (pre==nullptr){
            ScanDataChunk(doubleVector, *resVector, pre, static_cast<uint64_t>(cnt));
          }
          else if (pre->size()==0){
            // skip
          }
          else {
            ScanDataChunk(doubleVector, *resVector, pre, static_cast<uint64_t>(cnt));
          }
          break;
        }
        case orc::BYTE:
        case orc::INT:
        case orc::SHORT:
        case orc::LONG:
        {
          orc::LongVectorBatch* longVector = dynamic_cast<orc::LongVectorBatch*>(coldata);
          orc::LongVectorBatch* resVector = dynamic_cast<orc::LongVectorBatch*>(colres);
          if (pre==nullptr){
            ScanDataChunk(longVector, *resVector, pre, static_cast<uint64_t>(cnt));
          }
          else if (pre->size()==0){
            // skip
          }
          else {
            ScanDataChunk(longVector, *resVector, pre, static_cast<uint64_t>(cnt));
          }
          break;
        }
        case orc::STRING:
        case orc::CHAR:
        case orc::VARCHAR:
        case orc::BINARY:
        {
          orc::StringVectorBatch* stringVector = dynamic_cast<orc::StringVectorBatch*>(coldata);
          orc::StringVectorBatch* resVector = dynamic_cast<orc::StringVectorBatch*>(colres);
          if (pre==nullptr){
            ScanDataChunk(stringVector, *resVector, pre, static_cast<uint64_t>(cnt));
          }
          else if (pre->size()==0){
            // skip
          }
          else {
            ScanDataChunk(stringVector, *resVector, pre, static_cast<uint64_t>(cnt));
          }
          break;
        }

        default:
          throw std::runtime_error(std::to_string(attr->getKind()) + " type is not supported yet.");
      }

    }

    batches += 1;
    rows += batch->numElements;

    cnt += (int)pre->size();
    cbegin = std::chrono::steady_clock::now();
    time_compute += std::chrono::duration_cast<std::chrono::microseconds>( cbegin-cend).count();
  }
  std::cout << "Rows: " << rows << std::endl;
  std::cout << "Batches: " << batches << std::endl;
  std::cout << "cnt: " << cnt << std::endl;
  std::cout << "time for loading: " << (double)time_load/1000.0 << std::endl;
  std::cout << "time for computation: " << (double)time_compute/1000.0 << std::endl;
  restructBatch->numElements= static_cast<uint64_t>(cnt);
  return restructBatch;
}

void queryStems(std::string comp){
  bool clearcache = true;

  // start q1
  std::cout << "============Starting query 1 =============="<< std::endl;

  std::vector<int> projs;
  projs.push_back(2);
  projs.push_back(3);
  std::vector<int> filters;
  filters.push_back(0);
  filters.push_back(1);
  std::cout << "start arrow scan-fitler: " << projs.size() << std::endl;
  std::vector<std::string> ops;
  ops.push_back("EQUAL");
  ops.push_back("EQUAL");

  std::vector<std::string> opands;

  opands.push_back("2452653");
  opands.push_back("12032");


  std::ofstream ofs("/proc/sys/vm/drop_caches");
  if (clearcache){
    sync();
    std::cout << "Clear cache ... "<<std::endl;
    ofs << "3" << std::endl;
  }


  std::cout << "start ORC scan-fitler: " << opands.size() << std::endl;
  auto begin = std::chrono::steady_clock::now();

  const char *file_name = "catalog_sales";
  const char *query = "tpcds/q1";


  auto imme = scanORCFile(getORCFile(file_name,comp), file_name, query, 20,  &projs, &filters, &ops, &opands);
  auto end = std::chrono::steady_clock::now();
  auto time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

//  auto res_col = dynamic_cast<orc::LongVectorBatch*> (imme->fields[0]);
  std::cout << "number of cols in result: " <<imme->fields.size()<< " length:"<< imme->numElements << std::endl;
//  std::cout <<"length: "<< res_col->numElements<<" and first val:"<< res_col->data[0]<<std::endl;

  std::cout << "Query run time ORC: " << time_arrow<<std::endl;


  // start q2
  std::cout << "============Starting query 2 =============="<<std::endl;

  std::vector<int> sprojs;
  sprojs.push_back(0);
  sprojs.push_back(8);
  std::vector<int> sfilters;
  sfilters.push_back(1);
  sfilters.push_back(3);
  std::cout << "start arrow scan-fitler: " << sprojs.size() << std::endl;
  std::vector<std::string> sops;
  sops.push_back("EQUAL");
  sops.push_back("EQUAL");

  std::vector<std::string> sopands;
  sopands.push_back("F");
  sopands.push_back("Secondary");

  if (clearcache){
    std::cout << "Clear cache ... "<<std::endl;
    sync();ofs << "3" << std::endl;
  }


  file_name = "customer_demographics";
  query = "tpcds/q2";

  std::cout << "start ORC scan-fitler: " << sopands.size() << std::endl;
  begin = std::chrono::steady_clock::now();
  auto simme = scanORCFile(getORCFile(file_name,comp), file_name, query, 137200,  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

//  auto res_col = dynamic_cast<orc::LongVectorBatch*> (simme->fields[0]);
  std::cout << "number of cols in result: " <<simme->fields.size()<< " length:"<< simme->numElements <<std::endl;
//  <<"length: "<< res_col->numElements<<" and first val:"<< res_col->data[0]<<std::endl;

  std::cout << "Query run time ORC scan: " << time_arrow<<std::endl;




  // start q3
  std::cout << "============Starting query 3 =============="<<std::endl;

  sprojs.clear();
  sprojs.push_back(0);
  sfilters.clear();
  sfilters.push_back(1);
  sfilters.push_back(2);
  sfilters.push_back(3);
  std::cout << "start arrow scan-fitler: " << sprojs.size() << std::endl;
  sops.clear();
  sops.push_back("EQUAL");
  sops.push_back("EQUAL");
  sops.push_back("EQUAL");

  sopands.clear();
  sopands.push_back("M");
  sopands.push_back("D");
  sopands.push_back("College");

  if (clearcache){
    std::cout << "Clear cache ... "<<std::endl;
    sync();
    ofs << "3" << std::endl;
  }

  file_name = "customer_demographics";
  query = "tpcds/q3";

  std::cout << "start ORC scan-fitler: " << sopands.size() << std::endl;
  begin = std::chrono::steady_clock::now();
  simme = scanORCFile(getORCFile(file_name,comp), file_name, query, 27440,  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

//  res_col = dynamic_cast<orc::LongVectorBatch*> (simme->fields[0]);
  std::cout << "number of cols in result: " <<simme->fields.size()<< " length:"<<  simme->numElements <<std::endl;
// <<"length: "<< res_col->numElements<<" and first val:"<< res_col->data[0]
  std::cout << "Query run time arrow parquet parquet-table: " << time_arrow<<std::endl;




  // start q4
  std::cout << "============Starting query 4 =============="<<std::endl;

  sprojs.clear();
  sprojs.push_back(0);
  sprojs.push_back(15);
  sprojs.push_back(23);
  sfilters.clear();
  sfilters.push_back(19);
  sfilters.push_back(26);
  std::cout << "start arrow scan-fitler: " << sprojs.size() << std::endl;
  sops.clear();
  sops.push_back("GREATER");
  sops.push_back("LESS");

  sopands.clear();
  sopands.push_back("80.0");
  sopands.push_back("500.0");

  if (clearcache){
    std::cout << "Clear cache ... "<<std::endl;
    sync();
    ofs << "3" << std::endl;
  }

  file_name = "catalog_sales";
  query = "tpcds/q4";

  std::cout << "start ORC scan-fitler: " << sopands.size() << std::endl;
  begin = std::chrono::steady_clock::now();
  simme = scanORCFile(getORCFile(file_name,comp), file_name, query, 3000000,  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

//  res_col = dynamic_cast<orc::LongVectorBatch*> (simme->fields[0]);
  std::cout << "number of cols in result: " <<simme->fields.size()<< " length:"<<  simme->numElements <<std::endl;
  //<<"length: "<< res_col->numElements<<" and first val:"<< res_col->data[0]

  std::cout << "Query run time ORC: " << time_arrow<<std::endl;



  // start q5
  std::cout << "============Starting query 5 =============="<<std::endl;

  sprojs.clear();
  sprojs.push_back(0);
  sprojs.push_back(15);
  sprojs.push_back(23);
  sprojs.push_back(29);
  sprojs.push_back(32);
  sprojs.push_back(33);
  sfilters.clear();
  sfilters.push_back(19);
  std::cout << "start orc scan-fitler: " << sprojs.size() << std::endl;
  sops.clear();
  sops.push_back("GREATER");

  sopands.clear();
  sopands.push_back("80.0");

  if (clearcache){
    std::cout << "Clear cache ... "<<std::endl;
    sync();
    ofs << "3" << std::endl;
  }


  file_name = "catalog_sales";
  query = "tpcds/q5";

  std::cout << "start orc scan-fitler: " << sopands.size() << std::endl;
  begin = std::chrono::steady_clock::now();
  simme = scanORCFile(getORCFile(file_name,comp), file_name, query, 3000000,  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

//  res_col = dynamic_cast<orc::LongVectorBatch*> (simme->fields[0]);
  std::cout << "number of cols in result: " <<simme->fields.size()<< " length:"<<  simme->numElements<<std::endl;
  //  <<"length: "<< res_col->numElements<<" and first val:"<< res_col->data[0]

  std::cout << "Query run time orc: " << time_arrow<<std::endl;

}


int main(int argc, char *argv[]) {
  queryStems("lz4");
  return 0;
}
