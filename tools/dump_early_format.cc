/*
g++ -g -Wall -Wextra -O3 --std=c++11 -o dump_early_format dump_early_format.cc -lgcrypt
 */

#include <iostream>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <stdint.h>
#include <string.h>

#include <gcrypt.h>

class NotImplementedError: public std::logic_error {
 public:
  explicit NotImplementedError(std::string what) :
      std::logic_error(what) {};
};

class InvalidStateError: public std::logic_error {
 public:
  explicit InvalidStateError(std::string what) :
      std::logic_error(what) {};
};

class CommandLineError: public std::runtime_error {
 public:
  explicit CommandLineError(std::string what) :
      std::runtime_error(what) {};
};

class InvalidDataError: public std::runtime_error {
 public:
  explicit InvalidDataError(std::string what) :
      std::runtime_error(what) {};
};

class Arguments {
 public:
  Arguments(int argc, const char** argv);
  std::ifstream* Input();
  std::ostream* Output();

  std::string infilename;
  std::string outfilename;

 private:
  std::ifstream infile;
  std::ofstream outfile;
};

typedef void (*dumper_t)(std::ifstream&, std::ostream&);
class Main {
 public:
  Main(int argc, const char** argv);
  int Run();

 private:
  void InitializeGCrypt();
  dumper_t GetDumper();
  Arguments args;
};

class Buffer {
 public:
  explicit Buffer(int initial_capacity=0)
      : buffer(nullptr)
      , buf_size(0)
      , buf_capacity(initial_capacity)
      , buf_pos(0) {
    if (buf_capacity > 0)
      buffer = new uint8_t[buf_capacity];
  }

  Buffer& operator=(const Buffer&) = delete;
  Buffer(const Buffer&) = delete;

  ~Buffer() {
    delete[] buffer;
  }

  void Read(std::istream& input, int amt) {
    if (size() + amt > capacity())
      Reserve(size() + amt);
    input.read((char*)(data() + size()), amt);
    buf_size += input.gcount();
  }

  void Reserve(int new_cap) {
    if (new_cap < size())
      return;
    uint8_t * new_data = new uint8_t[new_cap];
    memcpy(new_data, buffer, size());
    delete[] buffer;
    buffer = new_data;
    buf_capacity = new_cap;
  }

  void Resize(int new_size) {
    if (capacity() < new_size)
      Reserve(new_size);
    if (new_size < 0)
      new_size = 0;
    buf_size = new_size;
  }

  int Find(int ch, int start=0, int end=-1) {
    if (start < 0)
      start = 0;
    if (end < 0)
      end = size();
    if (end > size())
      end = size();
    for (int i = start; i < end; i++) {
      if (buffer[i] == ch)
        return i;
    }
    return -1;
  }

  void CheckBufPos() {
    if (buf_pos < 0 || buf_pos > buf_size)
      throw InvalidDataError("Buffer position out of range");
  }
  void Seek(int new_pos) { buf_pos = new_pos; CheckBufPos(); }
  int IsAtEnd() const { return buf_pos >= buf_size; }
  int CurrentOctet() const { return (buffer)[buf_pos]; }
  int ReadOctet() { buf_pos += 1; CheckBufPos(); return (buffer)[buf_pos-1]; }
  long long int ReadVarUint();
  uint8_t * CurrentData() { return buffer + buf_pos; }
  char* CurrentDataChar() { return (char*)(buffer + buf_pos); }
  int CurrentDataSize() { return buf_size - buf_pos; }
  void Skip(int amt) { buf_pos += amt; CheckBufPos(); }
  uint32_t ReadUint32() {
    const uint8_t * d = CurrentData();
    buf_pos += 4;
    return d[0] | (d[1] << 8) | (d[2] << 16) | (d[3] << 24);
  }

  uint8_t * data() { return buffer; };
  int size() { return buf_size; };
  int capacity() { return buf_capacity; };

 private:
  uint8_t * buffer;
  int buf_size;
  int buf_capacity;
  int buf_pos;
};

class Helpers {
 public:
  Helpers(std::ifstream& infile);
  void read_block_settings();
  void dump_settings_block(std::ostream& outfile);
  void dump_next_content_block(std::ostream& outfile);

  int blocksize;
  int blockdatasize;
  int blocksumsize;
  std::string blocksum;
  std::vector<uint8_t> (*blockalgo)(const uint8_t* data, int datalen);

 private:
  std::ifstream& f;

  void WriteDateTimeForSecondsAfterEpoch(
      std::ostream& output, long long int sae);
  void WriteHexEncoded(std::ostream& output, uint8_t* data, int datalen);
  std::unique_ptr<Buffer> ReadNextBlock();
};


int main(int argc, const char ** argv) {
  try {
    Main m(argc, argv);
    return m.Run();
  } catch (CommandLineError& e) {
    std::cout << "ERROR: " << e.what() << "\n";
  } catch (std::runtime_error& e) {
    std::cout << "ERROR: " << e.what() << "\n";
  }
}

/* --------------------------------------------------
 * Helper functions
 * --------------------------------------------------
 */

int string_to_int(char* str, int strsize) {
  int value = 0;
  for (int i = 0; i < strsize; i++) {
    if (str[i] < '0' || str[i] > '9')
      throw std::runtime_error(
          "Could not parse string as value: " + std::string(str, strsize));
    value *= 10;
    value += str[i] - '0';
  }
  return value;
}

std::vector<uint8_t> calculate_unknown_checksum(
    const uint8_t* /*data*/, int /*datalen*/) {
  throw InvalidStateError("Checksum algorithm not initialized");
}

std::vector<uint8_t> calculate_sha256(const uint8_t* data, int datalen) {
  if (gcry_md_get_algo_dlen(GCRY_MD_SHA256) != 32)
    throw std::runtime_error(
        "gcrypt says sha256 is " +
        std::to_string(gcry_md_get_algo_dlen(GCRY_MD_SHA256)) + " bytes long!");
  gcry_md_hd_t hd;
  gcry_error_t err = gcry_md_open(&hd, GCRY_MD_SHA256, 0);
  if (gcry_err_code(err) != GPG_ERR_NO_ERROR)
    throw std::runtime_error("gcry_md_open returned an error");
  gcry_md_write(hd, (char*)data, datalen);
  unsigned char* digest = gcry_md_read(hd, GCRY_MD_SHA256);
  std::vector<uint8_t> digestvec((uint8_t*)digest, (uint8_t*)(digest+32));
  gcry_md_close(hd);
  return digestvec;
}

/* --------------------------------------------------
 * dumper for content
 * --------------------------------------------------
 */

void dump_content_file(std::ifstream& infile, std::ostream& outfile) {
  Helpers helpers(infile);
  helpers.read_block_settings();
  helpers.dump_settings_block(outfile);
  while (!infile.eof()) {
    if (!infile.good())
      throw std::runtime_error("Input file not in good state!");
    helpers.dump_next_content_block(outfile);
  }
}

/* --------------------------------------------------
 * class Arguments
 * --------------------------------------------------
 */

Arguments::Arguments(int argc, const char** argv) {
  int arg = 1;
  while (arg < argc) {
    std::string argstr(argv[arg]);
    if (argstr == "-o" || argstr == "--output") {
      if (arg + 1 >= argc)
        throw CommandLineError("Option '" + argstr + "' requires an argument");
      std::string ofn(argv[arg+1]);
      if (!outfilename.empty())
        throw CommandLineError(
            "Output file name set twice: " + outfilename + " and " + ofn);
      outfilename = ofn;
      arg += 2;
    } else if (argstr[0] == '-') {
      throw CommandLineError("Unknown option: " + argstr);
    } else {
      if (!infilename.empty())
        throw CommandLineError(
            "Input file name set twice: " + infilename + " and " + argstr);
      infilename = argstr;
      arg += 1;
    }
  }

  if (infilename.empty())
    throw CommandLineError("Required argument missing: input file name");
}

std::ifstream* Arguments::Input() {
  if (infile.is_open())
    return &infile;
  infile.open(infilename, std::ios_base::in | std::ios_base::binary);
  return &infile;
}

std::ostream* Arguments::Output() {
  if (outfile.is_open())
    return &outfile;
  if (outfilename.empty())
    return &std::cout;
  outfile.open(
      outfilename,
      std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
  return &outfile;
}

/* --------------------------------------------------
 * class Main
 * --------------------------------------------------
 */

Main::Main(int argc, const char** argv)
    : args(argc, argv) {
  InitializeGCrypt();
}

void Main::InitializeGCrypt() {
  // Mostly Copied from the gcrypt documentation.
  /* Version check should be the very first call because it
     makes sure that important subsystems are intialized. */
  if (!gcry_check_version (GCRYPT_VERSION))
    throw std::runtime_error("libgcrypt version mismatch\n");

  /* Disable secure memory.  */
  gcry_control (GCRYCTL_DISABLE_SECMEM, 0);

  /* Tell Libgcrypt that initialization has completed. */
  gcry_control (GCRYCTL_INITIALIZATION_FINISHED, 0);
}

int Main::Run() {
  dumper_t dumper = GetDumper();
  std::ostream* output = args.Output();
  *output << "event: dump start\n";
  dumper(*args.Input(), *output);
  *output << "event: dump complete\n";
  return 1;
}

dumper_t Main::GetDumper() {
  std::ifstream* infile = args.Input();
  infile->seekg(0);
  char buf[100];
  infile->read(buf, 100);
  int bufsize = infile->gcount();
  std::string start(buf, bufsize);
  if (start.find("ebakup content data\n") == 0)
    return dump_content_file;
  throw std::runtime_error(
      "Failed to recognize the file type of the input file (" +
      args.infilename + ")");
}

/* --------------------------------------------------
 * class Buffer
 * --------------------------------------------------
 */

long long int Buffer::ReadVarUint() {
  long long int value = 0;
  while (buf_pos < buf_size) {
    value <<= 7;
    value |= buffer[buf_pos] & 0x7f;
    buf_pos += 1;
    if (buffer[buf_pos-1] < 0x80)
      return value;
  }
  throw InvalidDataError("Varuint didn't end before the buffer");
}

/* --------------------------------------------------
 * class Helpers
 * --------------------------------------------------
 */
Helpers::Helpers(std::ifstream& infile)
    : blocksize(0)
    , blockdatasize(0)
    , blocksumsize(0)
    , blockalgo(calculate_unknown_checksum)
    , f(infile) {
}

void Helpers::read_block_settings() {
  char buf[10000];
  f.seekg(0);
  f.read(buf, 10000);
  int bufsize = f.gcount();
  char* sizestr = (char*)memmem(buf, bufsize, "\nedb-blocksize:", 15);
  if (!sizestr)
    throw InvalidDataError("No blocksize specified in data file");
  sizestr += 15;
  char* sizeend = (char*)memchr(sizestr, '\n', bufsize - (sizestr - buf));
  if (!sizeend)
    throw InvalidDataError("Failed to find end of blocksize value");
  blocksize = string_to_int(sizestr, sizeend - sizestr);
  if (sizeend - buf > blocksize)
    throw InvalidDataError("No blocksize specified in settings block");
  char* sumstr = (char*)memmem(buf, bufsize, "\nedb-blocksum:", 14);
  if (!sumstr || (sumstr - buf) > blocksize)
    throw InvalidDataError("No block checksum specified in settings block");
  sumstr += 14;
  char* sumend = (char*)memchr(sumstr, '\n', bufsize - (sumstr - buf));
  if (!sumend)
    throw InvalidDataError("Failed to find end of block checksum value");
  blocksum = std::string(sumstr, sumend - sumstr);
  if (blocksum == "sha256") {
    blocksumsize = 32;
    blockalgo = calculate_sha256;
  } else
    throw NotImplementedError("Unknown block checksum: " + blocksum);
  blockdatasize = blocksize - blocksumsize;
}

void Helpers::dump_settings_block(std::ostream& output) {
  f.seekg(0);
  std::unique_ptr<Buffer> block = ReadNextBlock();
  int done = 0;
  uint8_t * data = block->data();
  while (done < block->size()) {
    if (data[done] == 0) {
      while (done < block->size()) {
        if (data[done] != 0)
          throw InvalidDataError("Trailing garbage in settings block");
        done += 1;
      }
      return;
    }
    int end = block->Find('\n', done);
    if (end < 0)
      throw InvalidDataError("Failed to find end of setting");
    if (done == 0)
      output << "type: ";
    else {
      if (block->Find(':', done, end) < 0)
        throw InvalidDataError("No ':' in setting line");
      output << "setting: ";
    }
    output.write((char*)data + done, end - done + 1);
    done = end + 1;
  }
}

void Helpers::dump_next_content_block(std::ostream& output) {
  std::unique_ptr<Buffer> block = ReadNextBlock();
  block->Seek(0);
  while (!block->IsAtEnd()) {
    if (block->CurrentOctet() == 0xdd) {
      block->Skip(1);
      int cidlen = block->ReadVarUint();
      int sumlen = block->ReadVarUint();
      int cslen = std::max(cidlen, sumlen);
      output << "cid: ";
      WriteHexEncoded(output, block->CurrentData(), cidlen);
      output << "\nchecksum: ";
      if (cidlen == sumlen)
        output << "*";
      else
        WriteHexEncoded(output, block->CurrentData(), sumlen);
      output << "\n";
      block->Skip(cslen);
      long long int first = block->ReadUint32();
      long long int last = block->ReadUint32();
      output << "first: ";
      WriteDateTimeForSecondsAfterEpoch(output, first);
      output << "\nlast: ";
      WriteDateTimeForSecondsAfterEpoch(output, last);
      output << "\n";
      while (block->CurrentOctet() == 0xa0 || block->CurrentOctet() == 0xa1) {
        if (block->CurrentOctet() == 0xa1) {
          output << "changed: ";
          WriteHexEncoded(output, block->CurrentData(), sumlen);
          output << "\n";
        } else
          output << "restored\n";
        first = block->ReadUint32();
        last = block->ReadUint32();
        output << "first: ";
        WriteDateTimeForSecondsAfterEpoch(output, first);
        output << "\nlast: ";
        WriteDateTimeForSecondsAfterEpoch(output, last);
        output << "\n";
      }
    } else if (block->CurrentOctet() == 0) {
      while (!block->IsAtEnd()) {
        if (block->ReadOctet() != 0)
          throw InvalidDataError("Trailing garbage in content block");
      }
      return;
    } else
      throw InvalidDataError(
          "Unknown data type: " + std::to_string(block->CurrentOctet()));
  }
}

const int seconds_per_minute = 60;
const int seconds_per_hour = seconds_per_minute * 60;
const int seconds_per_day = seconds_per_hour * 24;
const long long int seconds_per_year = seconds_per_day * 365;
const int days_per_month[] = { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
//const int seconds_per_leapyear = seconds_per_day * 366;
void Helpers::WriteDateTimeForSecondsAfterEpoch(
    std::ostream& output, long long int sae) {
  if (sae < 0)
    throw NotImplementedError("Negative time stamps are not correctly handled");
  int years_no_leap = sae / seconds_per_year;
  long long int left = sae - years_no_leap * seconds_per_year;
  int leap_years = (years_no_leap + 2) / 4;
  leap_years -= (years_no_leap + 70) / 100;
  leap_years += (years_no_leap + 370) / 400;
  while (leap_years * seconds_per_day > left) {
    years_no_leap -= 1;
    left = sae - years_no_leap * seconds_per_year;
    leap_years = (years_no_leap + 2) / 4;
    leap_years -= (years_no_leap + 70) / 100;
    leap_years += (years_no_leap + 370) / 400;
  }
  int year = 1970 + years_no_leap;
  left -= leap_years * seconds_per_day;
  int days = left / seconds_per_day;
  left -= days * seconds_per_day;
  int day = days + 1;
  if (day > 59) {
    if (!(year % 400 == 0 || (year % 4 == 0 && year % 100 != 0)))
      day += 1;
  }
  int month = 0;
  int day_of_month = day;
  while (month < 11 && day_of_month > days_per_month[month]) {
    day_of_month -= days_per_month[month];
    month += 1;
  }
  month += 1;
  int hour = left / seconds_per_hour;
  left -= hour * seconds_per_hour;
  int minute = left / seconds_per_minute;
  int second = left - minute * seconds_per_minute;
  const char* ymsep = month < 10 ? "-0" : "-";
  const char* mdsep = day_of_month < 10 ? "-0" : "-";
  const char* dhsep = hour < 10 ? " 0" : " ";
  const char* hmsep = minute < 10 ? ":0" : ":";
  const char* mssep = second < 10 ? ":0" : ":";
  output << year << ymsep << month << mdsep << day_of_month << dhsep <<
      hour << hmsep << minute << mssep << second;
}

const char hexits[] = {
  '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
  'a', 'b', 'c', 'd', 'e', 'f' };
void Helpers::WriteHexEncoded(
    std::ostream& output, uint8_t* data, int datalen) {
  for (int i = 0; i < datalen; i++) {
    char n[2];
    n[0] = hexits[(data[i] >> 4) & 15];
    n[1] = hexits[data[i] & 15];
    output.write(n, 2);
  }
}

std::unique_ptr<Buffer> Helpers::ReadNextBlock() {
  std::unique_ptr<Buffer> buf(new Buffer);
  buf->Read(f, blocksize);
  if (buf->size() == 0) {
    if (f.eof())
      return buf;
    throw std::runtime_error(
        "Got no data even though it is not the end of the file");
  }
  if (buf->size() < blocksize) {
    throw std::runtime_error(
        "Got incomplete block (" + std::to_string(buf->size()) +
        " octets instead of " + std::to_string(blocksize) + ")\n");
  }
  if (buf->size() != blocksize) {
    throw std::runtime_error(
        "Got wrong amount of block data (" + std::to_string(buf->size()) +
        " octets instead of " + std::to_string(blocksize) + ")\n");
  }
  std::vector<uint8_t> checksum = blockalgo(buf->data(), blockdatasize);
  if (checksum.size() != (unsigned int)blocksumsize)
    throw std::runtime_error(
        "Block checksum did not have expected size (" +
        std::to_string(checksum.size()) + " vs " +
        std::to_string(blocksumsize));
  if (memcmp(checksum.data(), buf->data() + blockdatasize, blocksumsize) != 0)
    throw std::runtime_error("Block checksum mismatch!");
  buf->Resize(blockdatasize);
  return buf;
}
