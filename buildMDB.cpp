#include <cmath>
#include <string>
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
#include "jtb/jtbstr.h"
#include "jtb/jtbvec.h"

enum { THREADLIMIT = 30000 };

namespace fs = std::filesystem;
using st = std::vector<std::string>::size_type;

struct Film {
    JTB::Str tconst = "N\\a";
    JTB::Str title = "N\\a";
    JTB::Str origtitle = "N\\a";
    JTB::Str year = "N\\a";
    JTB::Str length = "N\\a";
    JTB::Str genre = "N\\a";
    JTB::Str rating = "0";
    JTB::Str numrates = "0";
    JTB::Str lang = "N\\a";
    JTB::Str directors = "";
    JTB::Str writers = "";
    JTB::Str actors = "";
};

struct Ratepair {
    std::string rating;
    std::string numrates;
};

const int nofdsets = 5;

void loadBasics(std::map<JTB::Str, Film>& film_hashmap, std::ifstream& filestream) {
    /* throwing out the first line */
    JTB::Str buf {};
    buf.absorbLine(filestream).clear();
    
    /* buffer variables */
    enum Cols { TCONST, TYPE, PRIMARY, ORIGINAL, ISADULT, STARTYEAR, ENDYEAR, RUNTIME, GENRES };

    /* processing the data */
    std::vector<std::thread> threadPack {};
    std::mutex mutex {};
    int count = 0;

    while (filestream.good()) {
	buf.clear().absorbLine(filestream);
	JTB::Vec<JTB::Str> rowslicer = buf.split("\t");
	if (rowslicer[TYPE] == "movie" 
	    && rowslicer[ISADULT] == "0"
	    && rowslicer[STARTYEAR] != R"(\N)" 
	    && rowslicer[RUNTIME] != R"(\N)") {

	    threadPack.emplace_back([&, rowslicer]() {
		Film film; 
		film.tconst = rowslicer.at(TCONST);
		film.title = rowslicer.at(PRIMARY);
		film.origtitle = rowslicer.at(ORIGINAL);
		film.year = rowslicer.at(STARTYEAR);
		film.length = rowslicer.at(RUNTIME);
		film.genre = rowslicer.at(GENRES);
		std::lock_guard<std::mutex> lock(mutex);
		film_hashmap[film.tconst] = film;
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

void loadRatings(std::map<JTB::Str, Film>& film_hashmap, std::ifstream& filestream) {
    /* buffers */
    JTB::Vec<JTB::Str> rowslicer {};
    JTB::Str buf {};

    /* throwing out first line */
    buf.absorbLine(filestream).clear();
    enum Cols { TCONST, RATING, NUMRATES };

    /* reading ratings into fdb */
    while (filestream.good()) {
        rowslicer = buf.clear().absorbLine(filestream).split("\t");
	try {
	    film_hashmap[rowslicer[TCONST]].rating = rowslicer[RATING];
	    film_hashmap[rowslicer[TCONST]].numrates = rowslicer[NUMRATES];
	} catch (std::exception e) { 
	    std::cerr << "Error: " << e.what() << '\n';
	}
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

void loadLanguage(std::map<JTB::Str, Film>& film_hashmap, std::ifstream& filestream) {
    /* buffers */
    JTB::Vec<JTB::Str> rowslicer {};
    JTB::Str buf {};

    enum Cols { TCONST, LANG };

    /* reading langs into buffer */
    while (filestream.good()) {
        rowslicer = buf.clear().absorbLine(filestream).split("\t");
	try { 
	    film_hashmap[rowslicer[TCONST]].lang = rowslicer[LANG];
	} catch (std::exception e) { 
	    std::cerr << "Error: " << e.what() << '\n';
	}
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


void loadPrincipals(std::map<JTB::Str, Film>& film_hashmap, std::ifstream& principals, std::ifstream& names) {
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
