#include <cmath>
#include <utility>
#include <vector>
#include <map>
#include <iostream>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <array>
#include <stdexcept>
#include <cstdlib>
#include <thread>

enum { THREADLIMIT = 30000 };

namespace fs = std::filesystem;
using st = std::vector<std::string>::size_type;

/* a structure for chopping up a tsv row */
struct TSVrow {
    private:
	std::vector<std::string> data;
	st cols = 0;

    public:
    TSVrow() = default;
    TSVrow(const std::string s) { parse(s); }

    TSVrow& parse(std::string s) {
	data.clear();

	std::istringstream ss(s); 
	std::stringstream chunk;
	char c = 0;
	do {
	    ss.get(c);
	    if (c != '\t' && ss) { chunk << c; }
	    else { data.push_back(chunk.str()); chunk.str(""); cols++; }
	} while (ss);
	return *this;
    }

    std::string operator[](st i) { return data[i]; }
    std::string operator[](st i) const { return data[i]; }

    st size() { return cols; }
    [[nodiscard]] st size() const { return cols; }

    std::vector<std::string>::iterator begin() { return data.begin(); }
    std::vector<std::string>::iterator end() { return data.end(); }
};

std::ostream& operator<<(std::ostream& os, TSVrow& row) {
    for (st i = 0; i < row.size()-1; i++) { os << row[i] << '\t'; }
    os << row[row.size()-1] << '\n';
    return os;
}

/* a structure for managing a dataset filestream */
class Dset {

    bool fnd = false;
    std::ifstream *filePtr = nullptr;
    std::string filename;

    public:

    Dset() = default;;
    Dset(const char *s): filename(s) {  };

    ~Dset() { delete filePtr; }

    bool setFoundToTrue() { fnd = true; return fnd; }
    bool getOpened() { return fnd; }

    void open(std::string s) { 
	filePtr = new std::ifstream(s); 
	if (!filePtr) throw std::runtime_error("Out of memory."); 
	setFoundToTrue(); 
    }

    [[nodiscard]] std::string getFilename() const {
	return filename;
    }

    bool getline(std::string& s) { std::getline(*filePtr, s); return (*filePtr).good(); }
    operator bool() const { return (*filePtr).good(); }
    std::string getline() { std::string s; std::getline(*filePtr, s); return s; }
};

struct Film {
    std::string tconst = "N\\a";
    std::string title = "N\\a";
    std::string origtitle = "N\\a";
    std::string year = "N\\a";
    std::string length = "N\\a";
    std::string genre = "N\\a";
    std::string rating = "0";
    std::string numrates = "0";
    std::string lang = "N\\a";
    std::string directors = "";
    std::string writers = "";
    std::string actors = "";
};

struct Ratepair {
    std::string rating;
    std::string numrates;
};

const int nofdsets = 5;

void loadBasics(std::map<std::string, Film>& fdb, Dset& dset) {
    TSVrow rowslicer;
    /* throwing out the first line */
    dset.getline();
    /* buffer variables */
    std::string linebuffer;
    enum Cols { TCONST, TYPE, PRIMARY, ORIGINAL, ISADULT, STARTYEAR, ENDYEAR, RUNTIME, GENRES };

    /* processing the data */
    std::vector<std::thread> threadPack;
    std::mutex mutex;
    int count = 0;

    while (dset.getline(linebuffer)) {
        rowslicer.parse(linebuffer);
	if (rowslicer[TYPE] == "movie" 
	    && rowslicer[ISADULT] == "0"
	    && rowslicer[STARTYEAR] != R"(\N)" 
	    && rowslicer[RUNTIME] != R"(\N)") {

	    threadPack.emplace_back([&, rowslicer]() {
		Film film; 
		film.tconst = rowslicer[TCONST];
		film.title = rowslicer[PRIMARY];
		film.origtitle = rowslicer[ORIGINAL];
		film.year = rowslicer[STARTYEAR];
		film.length = rowslicer[RUNTIME];
		film.genre = rowslicer[GENRES];
		std::lock_guard<std::mutex> lock(mutex);
		fdb[film.tconst] = film;
	    });
	    if ((++count)%THREADLIMIT == 0) {
		for (auto& thread : threadPack) {
		    thread.join();
		}
		threadPack.clear();
	    }
	}
    }
    for (auto& thread : threadPack) {
	thread.join();
    }
    std::cout << "Done reading the basics!" << '\n';
}

void loadRatings(std::map<std::string, Film>& fdb, Dset& dset) {
    /* buffers */
    TSVrow rowslicer;
    /* throwing out first line */
    dset.getline();

    /* reading ratings into fdb */
    std::string s;
    while (dset.getline(s)) {
        rowslicer.parse(s);
	try {
	    fdb.at(rowslicer[0]).rating = rowslicer[1];
	    fdb.at(rowslicer[0]).numrates = rowslicer[2];
	} catch (std::exception e) { }
    }

    /* erasing films with no rating */
    /* for (auto it = fdb.begin(); it != fdb.end();) { */
    /*     if (it->second.rating == "0" || it->second.numrates == "0") */
    /*         it = fdb.erase(it); */
	/* else */ 
	    /* it++; */
    /* } */

    std::cout << "Done reading ratings!" << '\n';
}

void loadLanguage(std::map<std::string, Film>& fdb, Dset& dset) {
    /* buffers */
    TSVrow row;
    std::string s;
    /* reading langs into buffer */
    while (dset.getline(s)) {
        row.parse(s);
	try { 
	    fdb.at(row[0]).lang = row[1];
	} catch (std::exception e) {  }
    }

    /* std::string lang; */
    /* buffer for langs */
    /* std::map<std::string, std::string> langs; */


    /* /1* transferring langs to fdb *1/ */
    /* for (auto& [k, film] : fdb) { */
	/* try { */
	    /* film.lang = langs[film.tconst]; */
	/* } catch (std::exception e) {  } */
	/* /1* lang = langs[film.tconst]; *1/ */
    /*     /1* if (!lang.empty()) { *1/ */
    /*         /1* film.lang = lang; *1/ */
    /*     /1* } *1/ */
    /* } */
    std::cout << "Done reading languages!" << '\n';
};

const int MAXBUF = 100000;


void loadPrincipals(std::map<std::string, Film>& fdb, Dset& principals, Dset& names) {
    /* buffers */
    TSVrow row;
    std::string s;

    /* buffer for names */
    std::map<std::string, std::string> namebuf;

    /* reading names into buffer */
    while (names.getline(s)) {
        row.parse(s);
	namebuf[row[0]] = row[1];
    }
    std::cout << "Done reading names!" << '\n';


    /* filling in principals */
    principals.getline(s);
    row.parse(s);
    bool notin = false;
    while (principals.getline(s)) {
	row.parse(s);
	if (notin && row[1] != "1") continue;
	try {
	    fdb.at(row[0]);
	    notin = false;
	    if (row[3].at(0) == 'a') {
		fdb[row[0]].actors = fdb[row[0]].actors+namebuf[row[2]]+',';
	    }
	    else if (row[3].at(0) == 'd') {
		fdb[row[0]].directors = fdb[row[0]].directors+namebuf[row[2]]+',';
	    }
	    else if (row[3].at(0) == 'w') {
		fdb[row[0]].writers = fdb[row[0]].writers+namebuf[row[2]]+',';
	    }
	} catch (std::out_of_range e) { 
	    notin = true;
	}
    }
    std::cout << "Done reading principals!" << '\n';
}

int main() {

    std::map<std::string, Dset> dataSetMap { 
	    { "lang", "lang.tsv" }, 
	    { "basics", "title.basics.tsv" }, 
	    { "ratings", "title.ratings.tsv" }, 
	    { "principals", "title.principals.tsv" },
	    { "name", "name.basics.tsv" } 
    };

    /* reading the directory and opening the relevant files if they're found */
    auto environ = std::getenv("__MOVIE_DATABASE_PATH");

    std::stringstream movieDatabasePath { "" }; 
    movieDatabasePath << (environ == nullptr ? "" : environ);

    /* if the env var is not set we use current directory <= 02/24/24 12:22:49 */ 
    if (movieDatabasePath.str().empty()) {
	std::cout << "No __MOVIE_DATABASE_PATH env variable. Using current directory instead..." << '\n';
	movieDatabasePath << fs::current_path().string();
	std::cout << movieDatabasePath.str() << '\n';
    }

    environ = std::getenv("MOVIES");

    std::stringstream moviesWithPath { "" }; 
    moviesWithPath << (environ == nullptr ? "" : environ);

    /* if the env var is not set we use current directory<= 02/24/24 12:22:49 */ 
    if (moviesWithPath.str().empty()) {
	std::cout << "No MOVIES env variable. Creating file in current directory instead..." << '\n';
	moviesWithPath << fs::current_path().string() << "movies.tsv";
	std::cout << moviesWithPath.str() << '\n';
    }

    fs::directory_iterator dirIterator { fs::path(movieDatabasePath.str()) };

    for (auto const& directoryEntry : dirIterator) {
	/* buffer for current filename */
	std::string fileName(directoryEntry.path().filename());

	/* opening files with names matching dataset filenames */
	for (auto& [key, ds] : dataSetMap) {
	    if (ds.getFilename() == fileName) ds.open(directoryEntry.path());
	} 
    }

    /* checking for whether all the necessary files are present */
    for (auto& [key, ds] : dataSetMap) {
        if (!ds.getOpened()) { 
	    std::string error { "Didn't find all the files. Missing: " };
	    error.append(ds.getFilename());
	    throw std::logic_error(error); 
	    return 1; 
	}
    }

    std::map<std::string, Film> fdata;

    loadBasics(fdata, dataSetMap["basics"]);
    loadRatings(fdata, dataSetMap["ratings"]);
    loadLanguage(fdata, dataSetMap["lang"]);
    loadPrincipals(fdata, dataSetMap["principals"], dataSetMap["name"]);

    std::ofstream os(moviesWithPath.str());
    char tab = '\t';
    for (auto const& [k, film] : fdata) {
	if (film.numrates != "0") {
	    os << film.tconst << tab << film.title << ";" << film.origtitle << tab; 
	    os << film.year << tab << film.length << tab << film.genre << tab; 
	    os << film.rating << tab << film.numrates << tab << film.lang << tab << film.directors << tab;
	    os << film.actors << tab << film.writers << '\n';
	}
    }
    std::cout << "All done!" << '\n';
}
