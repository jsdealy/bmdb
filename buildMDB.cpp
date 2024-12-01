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

template <typename T>
int icast(T thing) {
    return static_cast<int>(thing);
}

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

    while ( filestream.good() && !buf.clear().absorbLine(filestream).isEmpty() ) {
	JTB::Vec<JTB::Str> rowslicer = buf.split("\t");
	try {
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
	} catch (std::exception e) {
	    std::cerr << "Error reading basics: " << e.what() << '\n';
	    std::cerr << "Count: " << count << '\n';
	    std::cerr << "Rowslicer: " << rowslicer << '\n';
	    exit(1);
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
    while ( filestream.good() && !buf.clear().absorbLine(filestream).isEmpty() ) {
        rowslicer = buf.split("\t");
	try {
	    if (film_hashmap.contains(rowslicer[TCONST])) {
		film_hashmap[rowslicer[TCONST]].rating = rowslicer[RATING];
		film_hashmap[rowslicer[TCONST]].numrates = rowslicer[NUMRATES];
	    }
	} catch (std::exception e) { 
	    std::cerr << "Problem inserting ratings" << '\n';
	    std::cerr << "Error: " << e.what() << '\n';
	    exit(1);
	}
    }

    std::cout << "Done reading ratings!" << '\n';
}

void loadLanguage(std::map<JTB::Str, Film>& film_hashmap, std::ifstream& filestream) {
    /* buffers */
    JTB::Vec<JTB::Str> rowslicer {};
    JTB::Str buf {};

    enum Cols { TCONST, LANG };

    /* reading langs into buffer */
    int count = 0;
    while ( filestream.good() && !buf.clear().absorbLine(filestream).isEmpty() ) {
        rowslicer = buf.split("\t");
	if (rowslicer.size() < 2) continue; 
	try { 
	    if (film_hashmap.contains(rowslicer[TCONST])) {
		film_hashmap.at(rowslicer[TCONST]).lang = rowslicer[LANG];
	    }
	} catch (std::out_of_range e) { 
	    std::cerr << "Problem inserting languages" << '\n';
	    std::cerr << "Error: " << e.what() << '\n';
	    std::cerr << "Rowslicer: " << rowslicer << '\n';
	    exit(1);
	}
    }

    std::cout << "Done reading languages!" << '\n';

};

void loadPrincipals(std::map<JTB::Str, Film>& film_hashmap, std::ifstream& principals_stream, std::ifstream& names_stream) {
    /* buffers */
    JTB::Vec<JTB::Str> rowslicer {};
    JTB::Str buf {};

    /* buffer for names */
    std::map<JTB::Str, JTB::Str> namebuf {};

    enum class Names { NCONST, NAME };
    enum class Principles { TCONST, ORDERING, NCONST, CATEGORY, JOB, CHARACTERS };
    

    /* reading names into buffer */
    while (names_stream.good() && !buf.clear().absorbLine(names_stream).isEmpty()) {
        rowslicer = buf.split("\t");
	if (rowslicer.size() < 2) continue;
	namebuf[rowslicer[static_cast<int>(Names::NCONST)]] = rowslicer[static_cast<int>(Names::NAME)];
    }
    std::cout << "Done reading names!" << '\n';

    /* filling in principals */
    buf.absorbLine(principals_stream);
    while (principals_stream.good() && !buf.clear().absorbLine(principals_stream).isEmpty()) {
	rowslicer = buf.split("\t");
	if (rowslicer.size() < 4) continue;
	if (!film_hashmap.contains(rowslicer[icast(Principles::TCONST)])) continue;
	if (rowslicer.at(icast(Principles::CATEGORY)).startsWith("a")) {
	    film_hashmap[rowslicer.at(icast(Principles::TCONST))].actors 
		= film_hashmap[rowslicer.at(icast(Principles::TCONST))].actors + namebuf[rowslicer.at(icast(Principles::NCONST))] + ',';
	}
	else if (rowslicer.at(icast(Principles::CATEGORY)).startsWith("d")) {
	    film_hashmap[rowslicer.at(icast(Principles::TCONST))].directors 
		= film_hashmap[rowslicer.at(icast(Principles::TCONST))].directors + namebuf[rowslicer.at(icast(Principles::NCONST))] + ',';
	}
	else if (rowslicer.at(icast(Principles::CATEGORY)).startsWith("w")) {
	    film_hashmap[rowslicer.at(icast(Principles::TCONST))].writers 
		= film_hashmap[rowslicer.at(icast(Principles::TCONST))].writers + namebuf[rowslicer.at(icast(Principles::NCONST))] + ',';
	}
    }
    std::cout << "Done reading principals!" << '\n';
}

int main() {

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

    std::ifstream lang_stream {};
    std::ifstream basics_stream {}; 
    std::ifstream ratings_stream {}; 
    std::ifstream principals_stream {};
    std::ifstream name_basics_stream {};
    try { 
	lang_stream.open( movieDatabasePath.str() + "/lang.tsv" );
	basics_stream.open( movieDatabasePath.str() + "/title.basics.tsv" ); 
	ratings_stream.open( movieDatabasePath.str() + "/title.ratings.tsv" ); 
	principals_stream.open( movieDatabasePath.str() + "/title.principals.tsv" );
	name_basics_stream.open( movieDatabasePath.str() + "/name.basics.tsv" );
    } catch (std::exception e) { 
	std::cerr << "Problem with opening filestreams" << '\n';
	std::cerr << "Error: " << e.what() << '\n';
	exit(1);
    }

    std::map<JTB::Str, Film> fdata {};

    loadBasics(fdata, basics_stream);
    loadRatings(fdata, ratings_stream);
    loadLanguage(fdata, lang_stream);
    loadPrincipals(fdata, principals_stream, name_basics_stream);

    std::ofstream os { moviesWithPath.str() };

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
