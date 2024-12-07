#include <SQLiteCpp/Database.h>
#include <cmath>
#include <algorithm>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <memory>
#include <cstdlib>
#include <thread>
#include "jtb/jtbstr.h"
#include "jtb/jtbvec.h"
#include <SQLiteCpp/SQLiteCpp.h>
#include "progressbar/include/progressbar.hpp"


const int THREADLIMIT = 4;
const int PRINCIPLES_BATCH_SIZE = 5000000;
const bool VERBOSE = false;

namespace fs = std::filesystem;
using st = std::vector<std::string>::size_type;


class Pbar {
private: 
    progressbar pbar;
public:
    Pbar(int total): pbar(total) {};
    void update() { pbar.update(); }
    void reset() { pbar.reset(); }
    void set_niter(int n) { pbar.set_niter(n); }
};

template <typename T>
int icast(T thing) {
    return static_cast<int>(thing);
}

class Filebuffer {
private:
    JTB::Vec<JTB::Vec<JTB::Str>>* buffer {};
    int chunksize {0};
public:
    Filebuffer(std::ifstream& filestream) {
	JTB::Str buf {};
	JTB::Vec<JTB::Str> rowslicer {};
	buffer = new JTB::Vec<JTB::Vec<JTB::Str>>;
	while ( filestream.good() && !buf.clear().absorbLine(filestream).isEmpty() ) {
	    rowslicer = buf.split("\t");
	    buffer->push(rowslicer);
	}
	chunksize = buffer->size()/THREADLIMIT;
    };
    ~Filebuffer() { delete buffer; };
    auto& getBuf() { return *buffer; }
    int getChunksize() { return chunksize; }
};

void loadBasics(SQLite::Database& db, std::ifstream& filestream) {
    /* throwing out the first line */
    JTB::Str buf {};
    buf.absorbLine(filestream).clear();
    
    /* enum for rowslicer <== 11/29/24 10:56:34 */ 
    enum Cols { TCONST, TYPE, PRIMARY, ORIGINAL, ISADULT, STARTYEAR, ENDYEAR, RUNTIME, GENRES };

    std::unique_ptr<JTB::Vec<JTB::Vec<JTB::Str>>> filebuffer {new JTB::Vec<JTB::Vec<JTB::Str>>};

    /* processing the data */
    while ( filestream.good() && !buf.clear().absorbLine(filestream).isEmpty() ) {
	JTB::Vec<JTB::Str> rowslicer = buf.split("\t");
	if (rowslicer[TYPE].startsWith("mo")
	    && rowslicer[ISADULT] == "0"
	    && rowslicer[STARTYEAR] != R"(\N)" 
	    && rowslicer[RUNTIME] != R"(\N)") {
	    (*filebuffer).push(rowslicer);
	}
    }

    JTB::Vec<std::thread> threadPack {};
    int size = (*filebuffer).size();
    int chunksize = (*filebuffer).size()/THREADLIMIT;
    Pbar pbar(size);
    std::mutex mutex {};

    for (int threadnum=0; threadnum < THREADLIMIT; ++threadnum) {
	threadPack.push([&,threadnum](){
	    SQLite::Statement film_insert {db, "INSERT INTO Films (tconst, title, originalTitle) VALUES (?, ?, ?)" };
	    SQLite::Statement year_insert {db, "INSERT INTO Years (tconst, year) VALUES (?, ?)" };
	    SQLite::Statement runtime_insert {db, "INSERT INTO Runtimes (tconst, runtimeInMin) VALUES (?, ?)" };
	    SQLite::Statement genre_insert {db, "INSERT INTO Genres (tconst, genre) VALUES (?, ?)" };
	    int start = chunksize*(threadnum);
	    int stop = chunksize*(threadnum+1);
	    for (int line = start; line < std::min(stop,size); ++line) {

		{
		    std::lock_guard<std::mutex> lock { mutex };
		    pbar.update();
		}
		/* std::cerr << threadnum << " : " << (float(line-start)/chunksize)*100 << '\n'; */
		try {
		    film_insert.reset();
		    film_insert.bind(1, (*filebuffer).at(line).at(TCONST).c_str());
		    film_insert.bind(2, (*filebuffer).at(line).at(PRIMARY).c_str());
		    film_insert.bind(3, (*filebuffer).at(line).at(ORIGINAL).c_str());
		    year_insert.reset();
		    year_insert.bind(1, (*filebuffer).at(line).at(TCONST).c_str());
		    year_insert.bind(2, (*filebuffer).at(line).at(STARTYEAR).c_str());
		    runtime_insert.reset();
		    runtime_insert.bind(1, (*filebuffer).at(line).at(TCONST).c_str());
		    runtime_insert.bind(2, std::stoi((*filebuffer).at(line).at(RUNTIME).c_str()));
		    film_insert.exec();
		    year_insert.exec();
		    runtime_insert.exec();
		    JTB::Vec<JTB::Str> genres = (*filebuffer).at(line).at(GENRES).split(",");
		    genres.forEach([&](JTB::Str genre) {
			    genre_insert.reset();
			    genre_insert.bind(1, (*filebuffer).at(line).at(TCONST).c_str());
			    genre_insert.bind(2, genre.c_str());
			    genre_insert.exec();
		    });
		} catch (SQLite::Exception& e) {
		    if (VERBOSE) {
			std::cerr << "Problem reading basics: " << e.what() << '\n';
			std::cerr << "Rowslicer: " << (*filebuffer).at(line) << '\n';
		    }
		} catch (std::exception& e) {
		    std::cerr << "Error reading basics: " << e.what() << '\n';
		    std::cerr << "Rowslicer: " << (*filebuffer).at(line) << '\n';
		    exit(1);
		}
	    }
	});
    }
    threadPack.forEach([&](std::thread& thread){
	if (thread.joinable()) {
	    thread.join();
	}
    });
    std::cerr << "\nDone reading the basics!" << '\n';
}

void loadRatings(SQLite::Database& db, std::ifstream& filestream) {
    /* buffers */
    JTB::Str buf {};

    /* throwing out first line */
    buf.absorbLine(filestream).clear();

    enum Cols { TCONST, RATING, NUMRATES };

    JTB::Vec<std::thread> threadPack {};

    Filebuffer filebuffer { filestream };
    int size = filebuffer.getBuf().size();
    Pbar pbar(size);
    std::mutex mutex {};

    for (int threadnum = 0; threadnum < THREADLIMIT; ++threadnum) {
	threadPack.push([&,threadnum](){
	    SQLite::Statement insert { db, "INSERT INTO Ratings (tconst, rating, numVotes) VALUES (?, ?, ?)" };
	    int start = filebuffer.getChunksize()*threadnum;
	    int stop = filebuffer.getChunksize()*(threadnum+1);
	    for (int line = start; line < std::min(stop,size); ++line) {
		{
		    std::lock_guard<std::mutex> lock { mutex };
		    pbar.update();
		}
		try {
		    insert.reset(); 
		    insert.bind(1,filebuffer.getBuf().at(line).at(TCONST).c_str());
		    insert.bind(2,std::stof(filebuffer.getBuf().at(line).at(RATING).c_str()));
		    insert.bind(3,std::stoi(filebuffer.getBuf().at(line).at(NUMRATES).c_str()));
		    insert.exec(); 
		} catch (SQLite::Exception& e) { 
		    if (VERBOSE) std::cerr << "Problem inserting ratings: " << e.what() << '\n';
		} catch (std::exception& e) {
		    std::cerr << "Error: " << e.what() << '\n';
		    exit(1);
		}
	    }
	});
    }
    threadPack.forEach([&](std::thread& thread) {
	if (thread.joinable()) {
	    thread.join();
	}
    });

    std::cerr << "\nDone reading ratings!" << '\n';
}

void loadLanguage(SQLite::Database& db, std::ifstream& langfilstream) {
    /* buffers */

    enum Cols { TCONST, LANG };

    Filebuffer filebuffer { langfilstream };
    JTB::Vec<std::thread> threadPack {};
    std::mutex mutex {};
    int size = filebuffer.getBuf().size();

    for (int threadnum = 0; threadnum < THREADLIMIT; ++threadnum) {
	int start = filebuffer.getChunksize()*threadnum;
	int stop = filebuffer.getChunksize()*(threadnum+1);
	threadPack.push([&,start,stop](){
	    SQLite::Statement insert { db, "INSERT INTO Languages (tconst, lang) VALUES (?, ?)" };
	    for (int line = start; line < std::min(stop,size); ++line) {
		if (filebuffer.getBuf().at(line).size() < 2) continue; 
		try { 
		    insert.reset(); 
		    insert.bind(1,filebuffer.getBuf().at(line).at(TCONST).c_str());
		    insert.bind(2,filebuffer.getBuf().at(line).at(LANG).c_str());
		    insert.exec(); 
		} catch (SQLite::Exception& e) { 
		    if (VERBOSE) std::cerr << "Problem: " << e.what() << '\n';
		} catch (std::exception& e) { 
		    std::cerr << "Error: " << e.what() << '\n';
		    exit(1);
		}
	    }
	});
	threadPack.forEach([&](std::thread& thread) {
	    if (thread.joinable()) {
		thread.join();
	    }
	});
    }
    std::cerr << "Done reading languages!" << '\n';
};


void loadPrincipals(SQLite::Database& db, std::ifstream& principals_stream, std::ifstream& names_stream) {
    /* buffers */
    /* throwing out the first line */
    JTB::Str buf {};
    buf.absorbLine(principals_stream).clear();
    enum class Names { NCONST, NAME };
    enum class Principles { TCONST, ORDERING, NCONST, CATEGORY, JOB, CHARACTERS };
    int runs = 0;

    while (names_stream.good()) {
	if (runs++ > 0) std::cerr << '\n';
	std::unique_ptr<JTB::Vec<JTB::Vec<JTB::Str>>> filebuffer {new JTB::Vec<JTB::Vec<JTB::Str>>};
	int linecount = 0;

	/* pushing into a buffer */
	while (names_stream.good() && ++linecount < PRINCIPLES_BATCH_SIZE && !buf.clear().absorbLine(names_stream).isEmpty()) {
	    JTB::Vec<JTB::Str> rowslicer = buf.split("\t");
	    if (rowslicer.size() < 2) continue;
	    (*filebuffer).push(rowslicer);
	}

	/* feeding into database <== 12/07/24 11:52:14 */ 
	JTB::Vec<std::thread> threadPack {};
	int size = (*filebuffer).size();
	int chunksize = (*filebuffer).size()/THREADLIMIT;
	Pbar pbar(size);
	std::mutex mutex {};

	for (int threadnum=0; threadnum < THREADLIMIT; ++threadnum) {
	    threadPack.push([&,threadnum](){
		int start = chunksize*(threadnum);
		int stop = chunksize*(threadnum+1);
		SQLite::Statement insert { db, "INSERT INTO Names (nconst, name) VALUES (?, ?)" };
		int min { std::min(stop,size) } ;
		for (int line = start; line < min; ++line) {
		    {
			std::lock_guard<std::mutex> lock { mutex };
			pbar.update();
		    }
		    /* std::cerr << threadnum << " : " << (float(line-start)/chunksize)*100 << '\n'; */
		    try {
			insert.reset(); 
			insert.bind(1,filebuffer->at(line).at(icast(Names::NCONST)).c_str());
			insert.bind(2,filebuffer->at(line).at(icast(Names::NAME)).c_str());
			insert.exec(); 
		    } catch (SQLite::Exception& e) { 
			if (VERBOSE) std::cerr << "Problem with Names: " << e.what() << '\n';
		    } catch (std::exception& e) {
			std::cerr << "Error: " << e.what() << '\n';
			exit(1);
		    }
		}
	    });
	}

	for (auto i = 0; i < threadPack.size(); ++i) {
	    if (threadPack.at(i).joinable()) {
		threadPack.at(i).join();
	    }
	};
    }

    std::cerr << "\nDone reading names!" << '\n';

    runs = 0;

    while (principals_stream.good()) {
	if (runs++ > 0) { std::cerr << '\n'; }
	std::unique_ptr<JTB::Vec<JTB::Vec<JTB::Str>>> filebuffer {new JTB::Vec<JTB::Vec<JTB::Str>>};
	int linecount = 0;

	/* pushing into a buffer */
	while (principals_stream.good() && ++linecount < PRINCIPLES_BATCH_SIZE && !buf.clear().absorbLine(principals_stream).isEmpty()) {
	    JTB::Vec<JTB::Str> rowslicer = buf.split("\t");
	    if (rowslicer.size() < 6) continue;
	    (*filebuffer).push(rowslicer);
	}

	/* feeding into database <== 12/07/24 11:52:14 */ 
	JTB::Vec<std::thread> threadPack {};
	int size = (*filebuffer).size();
	int chunksize = (*filebuffer).size()/THREADLIMIT;
	Pbar pbar(size);
	std::mutex mutex {};

	for (int threadnum=0; threadnum < THREADLIMIT; ++threadnum) {
	    threadPack.push([&,threadnum](){
		int start = chunksize*(threadnum);
		int stop = chunksize*(threadnum+1);
		SQLite::Statement directors_insert { db, "INSERT INTO Directors (tconst, nconst) VALUES (?, ?)" };
		SQLite::Statement actors_insert { db, "INSERT INTO Actors (tconst, nconst) VALUES (?, ?)" };
		SQLite::Statement writers_insert { db, "INSERT INTO Writers (tconst, nconst) VALUES (?, ?)" };
		JTB::Str skipbuf {};
		int min = std::min(stop,size);
		for (int line = start; line < min; ++line) {
		    JTB::Str tconst = filebuffer->at(line).at(icast(Principles::TCONST));
		    {
			std::lock_guard<std::mutex> lock {mutex};
			pbar.update();
		    }
		    if (tconst == skipbuf) continue;
		    /* std::cerr << threadnum << " : " << (float(line-start)/chunksize)*100 << '\n'; */
		    JTB::Str category = filebuffer->at(line).at(icast(Principles::CATEGORY));
		    try {
			/* actors <== 11/29/24 15:39:28 */ 
			if (category.startsWith("a")) {
			    try {
				actors_insert.reset();
				actors_insert.bind(1, tconst.c_str());
				actors_insert.bind(2, filebuffer->at(line).at(icast(Principles::NCONST)).c_str());
				actors_insert.exec();
			    } catch (SQLite::Exception& e) {
				if (VERBOSE) std::cerr << "actor excpt: " << e.what() << '\n';
				skipbuf.clear().push(tconst);
			    } catch (std::exception& e) {
				std::cerr << "error: " << e.what() << '\n';
				exit(1);
			    }
			}
			/* directors <== 11/29/24 15:39:32 */ 
			else if (category.startsWith("d")) {
			    try {
				directors_insert.reset();
				directors_insert.bind(1, tconst.c_str());
				directors_insert.bind(2, filebuffer->at(line).at(icast(Principles::NCONST)).c_str());
				directors_insert.exec();
			    } catch (SQLite::Exception& e) {
				if (VERBOSE) std::cerr << "director excpt: " << e.what() << '\n';
				skipbuf.clear().push(tconst);
			    } catch (std::exception& e) {
				std::cerr << "error: " << e.what() << '\n';
				exit(1);
			    }
			}
			/* writers <== 11/29/24 15:39:37 */ 
			else if (category.startsWith("w")) {
			    try {
				writers_insert.reset();
				writers_insert.bind(1, tconst.c_str());
				writers_insert.bind(2, filebuffer->at(line).at(icast(Principles::NCONST)).c_str());
				writers_insert.exec();
			    } catch (SQLite::Exception& e) {
				if (VERBOSE) std::cerr << "writer excpt: " << e.what() << '\n';
				skipbuf.clear().push(tconst);
			    } catch (std::exception& e) {
				std::cerr << "error: " << e.what() << '\n';
				exit(1);
			    }
			}
		    } catch (SQLite::Exception& e) {
			if (VERBOSE) std::cerr << "Problem with principals: " << e.what() << '\n';
		    } catch (std::exception& e) {
			std::cerr << "Error while inserting principals: " << e.what() << '\n';
		    }
		}
	    });
	}

	for (auto i = 0; i < threadPack.size(); ++i) {
	    if (threadPack.at(i).joinable()) {
		threadPack.at(i).join();
	    }
	};
    }
    std::cerr << "\nDone reading principals!" << '\n';
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
    } catch (std::exception& e) { 
	std::cerr << "Problem with opening filestreams" << '\n';
	std::cerr << "Error: " << e.what() << '\n';
	exit(1);
    }

    try {
	SQLite::Database db {movieDatabasePath.str() + "/moviedatabase.db", SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE};
	SQLite::Statement wal { db, "pragma journal_mode = WAL" };
	/* SQLite::Statement sync { db, "pragma synchronous = 0" }; */
	SQLite::Statement cache { db, "pragma cache_size = 1000000" };
	SQLite::Statement locking { db, "pragma locking_mode = NORMAL" };
	SQLite::Statement tempstore { db, "pragma temp_store = memory" };
	SQLite::Statement mmap { db, "pragma mmap_size = 30000000000" };
	SQLite::Statement foreign_keys { db, "pragma foreign_keys = on" };
	cache.executeStep(); 
	locking.executeStep();
	foreign_keys.executeStep();
	wal.executeStep(); 
	/* sync.executeStep(); */ 
	tempstore.executeStep(); mmap.executeStep();
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Films" (
	    tconst TEXT NOT NULL PRIMARY KEY,
	    title TEXT NOT NULL,
	    originalTitle TEXT NOT NULL))");
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Genres" (
	    tconst TEXT NOT NULL,
	    genre TEXT NOT NULL,
	    FOREIGN KEY (tconst) REFERENCES Films (tconst)))");
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Runtimes" (
	    tconst TEXT NOT NULL, 
	    runtimeInMin INT NOT NULL,
	    FOREIGN KEY (tconst) REFERENCES Films (tconst),
	    UNIQUE(tconst, runtimeInMin)))");
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Years" (
	    tconst TEXT NOT NULL,
	    year INT NOT NULL,
	    FOREIGN KEY (tconst) REFERENCES Films (tconst),
	    UNIQUE(tconst, year)))");
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Names" (
	    nconst TEXT NOT NULL PRIMARY KEY,
	    name TEXT NOT NULL,
	    UNIQUE(nconst, name)))");
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Directors" (
	    tconst TEXT NOT NULL, 
	    nconst TEXT NOT NULL, 
	    FOREIGN KEY (tconst) REFERENCES Films (tconst),
	    FOREIGN KEY (nconst) REFERENCES Names (nconst),
	    UNIQUE(tconst,nconst)))");
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Actors" (
	    tconst TEXT NOT NULL, 
	    nconst TEXT NOT NULL, 
	    FOREIGN KEY (tconst) REFERENCES Films (tconst),
	    FOREIGN KEY (nconst) REFERENCES Names (nconst),
	    UNIQUE(tconst,nconst)))");
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Writers" (
	    tconst TEXT NOT NULL, 
	    nconst TEXT NOT NULL, 
	    FOREIGN KEY (tconst) REFERENCES Films (tconst),
	    FOREIGN KEY (nconst) REFERENCES Names (nconst),
	    UNIQUE(tconst,nconst)))");
	/* db.exec(R"(CREATE TABLE IF NOT EXISTS "KnownFor" ( */
	/*     tconst TEXT NOT NULL, */ 
	/*     nconst TEXT NOT NULL, */ 
	/*     FOREIGN KEY (tconst) REFERENCES Films (tconst), */
	/*     FOREIGN KEY (nconst) REFERENCES Names (nconst), */
	/*     UNIQUE(tconst,nconst)))"); */
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Ratings" (
	    tconst TEXT NOT NULL UNIQUE, 
	    rating FLOAT NOT NULL,
	    numVotes INTEGER NOT NULL,
	    FOREIGN KEY (tconst) REFERENCES Films (tconst)))");
	db.exec(R"(CREATE TABLE IF NOT EXISTS "Languages" (
	    tconst TEXT NOT NULL, 
	    lang TEXT NOT NULL,
	    FOREIGN KEY (tconst) REFERENCES Films (tconst)))");

	loadBasics(db, basics_stream);
	loadRatings(db, ratings_stream);
	loadLanguage(db, lang_stream);
	loadPrincipals(db, principals_stream, name_basics_stream);
    } catch (std::exception& e) {
	std::cerr << e.what() << '\n';
	exit(1);
    }

    std::cout << "All done!" << '\n';
}
