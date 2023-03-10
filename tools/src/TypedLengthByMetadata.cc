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

#include <getopt.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>

#include "orc/OrcFile.hh"
#include "orc/Exceptions.hh"

//#include "Adaptor.hh"
#include "wrap/orc-proto-wrapper.hh"

long i_size = 0;
long d_size = 0;
long s_size = 0;

void printStripeInformation(std::ostream& out,
                            uint64_t index,
                            uint64_t columns,
                            std::unique_ptr<orc::StripeInformation> stripe,
                            const orc::Type& schema,
                            bool verbose) {
  out << "    { \"stripe\": " << index
      << ", \"rows\": " << stripe->getNumberOfRows() << ",\n";
  out << "      \"offset\": " << stripe->getOffset()
      << ", \"length\": " << stripe->getLength() << ",\n";
  out << "      \"index\": " << stripe->getIndexLength()
      << ", \"data\": " << stripe->getDataLength()
      << ", \"footer\": " << stripe->getFooterLength();
  if (verbose) {
    out << ",\n      \"encodings\": [\n";
    for(uint64_t col=0; col < columns; ++col) {
      if (col != 0) {
        out << ",\n";
      }
      orc::ColumnEncodingKind encoding = stripe->getColumnEncoding(col);
      out << "         { \"column\": " << col
          << ", \"encoding\": \""
          << columnEncodingKindToString(encoding) << "\"";
      if (encoding == orc::ColumnEncodingKind_DICTIONARY ||
          encoding == orc::ColumnEncodingKind_DICTIONARY_V2) {
        out << ", \"count\": " << stripe->getDictionarySize(col);
      }
      out << " }";
    }
    out << "\n      ],\n";
    out << "      \"streams\": [\n";
    long int_size = 0;
    long double_size = 0;
    long str_size = 0;

    for(uint64_t str = 0; str < stripe->getNumberOfStreams(); ++str) {
      if (str != 0) {
        out << ",\n";
      }
      ORC_UNIQUE_PTR<orc::StreamInformation> stream =
        stripe->getStreamInformation(str);
      out << "        { \"id\": " << str
          << ", \"column\": " << stream->getColumnId()
          << ", \"kind\": \"" << streamKindToString(stream->getKind())
          << "\", \"offset\": " << stream->getOffset()
//          << ", \"type\": " << schema.getSubtype(stream->getColumnId()-1)->toString()
          << ", \"length\": " << stream->getLength() << " }";
      if (stream->getColumnId() ==0){

      }
      else if (schema.getSubtype(stream->getColumnId()-1)->getKind()==orc::TypeKind::INT||schema.getSubtype(stream->getColumnId()-1)->getKind()==orc::TypeKind::LONG){
          int_size+=stream->getLength();
      }
      else if (schema.getSubtype(stream->getColumnId()-1)->getKind()==orc::TypeKind::DOUBLE||schema.getSubtype(stream->getColumnId()-1)->getKind()==orc::TypeKind::FLOAT){
          double_size+=stream->getLength();
      }
      else if (schema.getSubtype(stream->getColumnId()-1)->getKind()==orc::TypeKind::STRING||schema.getSubtype(stream->getColumnId()-1)->getKind()==orc::TypeKind::VARCHAR){
          str_size+=stream->getLength();
      }

    }
    out << "\n      ]";
    out  << "\n \"length\": " << stripe->getLength()<< ", \"sum length\": " << int_size+double_size+str_size << ",\n";
    std::string tz = stripe->getWriterTimezone();
    if (tz.length() != 0) {
      out << ",\n      \"timezone\": \"" << tz << "\"";
    }
    i_size += int_size;
    d_size += double_size;
    s_size +=str_size;
  }
  out << "\n    }";
}

void printRawTail(std::ostream& out,
                  const char*filename) {
  out << "Raw file tail: " << filename << "\n";
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readFile(filename), orc::ReaderOptions());
  // Parse the file tail from the serialized one.
  orc::proto::FileTail tail;
  if (!tail.ParseFromString(reader->getSerializedFileTail())) {
    throw orc::ParseError("Failed to parse the file tail from string");
  }
  out << tail.DebugString();
}

void printAttributes(std::ostream& out, const orc::Type& type,
    const std::string name, bool* hasAnyAttributes) {
  const auto& attributeKeys = type.getAttributeKeys();
  bool typeHasAttrs = !attributeKeys.empty();
  if (typeHasAttrs) {
    // 'hasAnyAttributes' is only needed to deal with commas properly.
    if (*hasAnyAttributes) {
      out << ',';
    } else {
      *hasAnyAttributes = true;
    }
    out << "\n    \"" << name << "\": {";
  }
  for (uint64_t i = 0; i < attributeKeys.size(); ++i) {
    const auto& key = attributeKeys[i];
    const auto& value = type.getAttributeValue(key);
    out << "\"" << key << "\": \"" << value << "\"";
    if (i < attributeKeys.size() - 1) {
      out << ", ";
    }
  }
  if (typeHasAttrs) {
    out << '}';
  }
  for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
    const auto& child = *type.getSubtype(i);
    std::string fieldName;
    if (type.getKind() == orc::STRUCT) {
      fieldName = type.getFieldName(i);
    } else if (type.getKind() == orc::LIST) {
      fieldName = "_elem";
    } else if (type.getKind() == orc::MAP) {
      fieldName = i == 0 ? "_key" : "_value";
    } else {
      fieldName = "_field_" + std::to_string(i);
    }
    std::string childName = (name.empty() ? "" : name + '.') + fieldName;
    printAttributes(out, child, childName, hasAnyAttributes);
  }
}

void printMetadata(std::ostream & out, const char*filename, bool verbose) {
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readFile(filename), orc::ReaderOptions());
  out << "{ \"name\": \"" << filename << "\",\n";
  uint64_t numberColumns = reader->getType().getMaximumColumnId() + 1;
  const orc::Type& schema = reader->getType();
  out << "  \"type\": \""
            << reader->getType().toString() << "\",\n";
  out << "  \"attributes\": {";
  bool hasAnyAttributes = false;
  printAttributes(out, reader->getType(), /*name=*/"", &hasAnyAttributes);
  out << "},\n";
  out << "  \"rows\": " << reader->getNumberOfRows() << ",\n";
  uint64_t stripeCount = reader->getNumberOfStripes();
  out << "  \"stripe count\": " << stripeCount << ",\n";
  out << "  \"format\": \"" << reader->getFormatVersion().toString()
      << "\", \"writer version\": \""
            << orc::writerVersionToString(reader->getWriterVersion())
            << "\", \"software version\": \"" << reader->getSoftwareVersion()
            << "\",\n";
  out << "  \"compression\": \""
            << orc::compressionKindToString(reader->getCompression())
            << "\",";
  if (reader->getCompression() != orc::CompressionKind_NONE) {
    out << " \"compression block\": "
              << reader->getCompressionSize() << ",";
  }
  out << "\n  \"file length\": " << reader->getFileLength() << ",\n";
  out << "  \"content\": " << reader->getContentLength()
      << ", \"stripe stats\": " << reader->getStripeStatisticsLength()
      << ", \"footer\": " << reader->getFileFooterLength()
      << ", \"postscript\": " << reader->getFilePostscriptLength() << ",\n";
  if (reader->getRowIndexStride()) {
    out << "  \"row index stride\": "
              << reader->getRowIndexStride() << ",\n";
  }
  out << "  \"user metadata\": {";
  std::list<std::string> keys = reader->getMetadataKeys();
  uint64_t remaining = keys.size();
  for(std::list<std::string>::const_iterator itr = keys.begin();
      itr != keys.end(); ++itr) {
    out << "\n    \"" << *itr << "\": \""
              << reader->getMetadataValue(*itr) << "\"";
    if (--remaining != 0) {
      out << ",";
    }
  }
  out << "\n  },\n";
  out << "  \"stripes\": [\n";
  for(uint64_t i=0; i < stripeCount; ++i) {
    printStripeInformation(out, i, numberColumns, reader->getStripe(i),schema,
                           verbose);
    if (i == stripeCount - 1) {
      out << "\n";
    } else {
      out << ",\n";
    }
  }
  out << "  ]\n";
  out << "}\n";
  out << "****"<< orc::compressionKindToString(reader->getCompression())<<","<<filename<<","<<i_size<<","<< d_size<<","<<s_size<<","<< i_size+d_size+s_size <<","<<reader->getFileLength() << "\n";
}

int main(int argc, char* argv[]) {
  static struct option longOptions[] = {
    {"help", no_argument, ORC_NULLPTR, 'h'},
    {"raw", no_argument, ORC_NULLPTR, 'r'},
    {"verbose", no_argument, ORC_NULLPTR, 'v'},
    {ORC_NULLPTR, 0, ORC_NULLPTR, 0}
  };
  bool helpFlag = false;
  bool verboseFlag = false;
  bool rawFlag = false;
  int opt;
  do {
    opt = getopt_long(argc, argv, "hrv", longOptions, ORC_NULLPTR);
    switch (opt) {
    case '?':
    case 'h':
      helpFlag = true;
      opt = -1;
      break;
    case 'v':
      verboseFlag = true;
      break;
    case 'r':
      rawFlag = true;
      break;
    }
  } while (opt != -1);
  argc -= optind;
  argv += optind;

  if (argc < 1 || helpFlag) {
    std::cerr
      << "Usage: orc-metadata [-h] [--help] [-r] [--raw] [-v] [--verbose]"
      << " <filename>\n";
    exit(1);
  } else {
    for(int i=0; i < argc; ++i) {
      try {
        if (rawFlag) {
          printRawTail(std::cout, argv[i]);
        } else {
          printMetadata(std::cout, argv[i], verboseFlag);
        }
      } catch (std::exception& ex) {
        std::cerr << "Caught exception in " << argv[i]
                  << ": " << ex.what() << "\n";
        return 1;
      }
    }
  }
  return 0;
}
