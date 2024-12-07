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

const int THREADLIMIT = 4;

namespace fs = std::filesystem;
using st = std::vector<std::string>::size_type;

uint convertToInt(JTB::Str& str) {
    std::string buf {};
    int sz = str.size();
    int i = 2;
    while (str.at(i) == '0') {++i;};
    return std::stoi(str.slice(i,sz).stdstr());
}

struct StreamData {
    uint linecount {};
    JTB::Vec<uint> starts {};
    JTB::Vec<uint> stops {};
};

StreamData getStreamData(std::ifstream& ifstream, int threadlimit, std::function<int(std::ifstream&,uint)> callback) {
    if (threadlimit <= 0) {
	std::cerr << "Called getStreamData with zero threadlimit" << '\n';
	exit(1);
    }

    StreamData streamdata {};
    uint max = 0;
    JTB::Str buf {};

    buf.absorbLine(ifstream).clear();
    while (ifstream.good()) { 
	try {
	    uint index = callback(ifstream,max);
	    max = max < index+1 ? index+1 : max;
	} catch (std::exception& e) {
	    std::cerr << "Exception while getting max: " << e.what() << '\n';
	}
    }
    streamdata.linecount = max;
    std::cerr << "max: " << max << '\n';

    ifstream.clear();
    ifstream.seekg(0, std::ios::beg);

    uint count = 0;
    buf.absorbLine(ifstream).clear();
    uint position_seeker {0};
    while (ifstream.good()) { 
	if (count % (streamdata.linecount/threadlimit) == 0) {
	    streamdata.starts.push(ifstream.tellg());
	    if (count > 0) streamdata.stops.push(static_cast<uint>(ifstream.tellg())-1);
	} 
	position_seeker = static_cast<uint>(ifstream.tellg());
	++count; buf.absorbLine(ifstream).clear();  
    }
    streamdata.stops.push(position_seeker);
    std::cerr << "starts: " << streamdata.starts << '\n';
    std::cerr << "stops: " << streamdata.stops << '\n';

    ifstream.clear();
    ifstream.seekg(0,std::ios::beg);
    return streamdata;
}


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

void loadBasics(std::vector<std::unique_ptr<SQLite::Database>>& dbs, std::ifstream& filestream) {
    /* throwing out the first line */
    JTB::Str buf {};
    buf.absorbLine(filestream).clear();
    
    /* enum for rowslicer <== 11/29/24 10:56:34 */ 
    enum Cols { TCONST, TYPE, PRIMARY, ORIGINAL, ISADULT, STARTYEAR, ENDYEAR, RUNTIME, GENRES };

    std::unique_ptr<JTB::Vec<JTB::Vec<JTB::Str>>> filebuffer {new JTB::Vec<JTB::Vec<JTB::Str>>};

    /* processing the data */
    while ( filestream.good() && !buf.clear().absorbLine(filestream).isEmpty() ) {
	JTB::Vec<JTB::Str> rowslicer = buf.split("\t");
	if (rowslicer[TYPE] == "movie" 
	    && rowslicer[ISADULT] == "0"
	    && rowslicer[STARTYEAR] != R"(\N)" 
	    && rowslicer[RUNTIME] != R"(\N)") {
	    (*filebuffer).push(rowslicer);
	}
    }

    JTB::Vec<std::thread> threadPack {};
    std::mutex mutex {};
    int size = (*filebuffer).size();
    int chunksize = (*filebuffer).size()/THREADLIMIT;

    for (int threadnum=0; threadnum < THREADLIMIT; ++threadnum) {
	threadPack.push([&,threadnum](){
	    SQLite::Statement film_insert {*(dbs.at(0)), "INSERT INTO Films (tconst, title, originalTitle) VALUES (?, ?, ?)" };
	    SQLite::Statement year_insert {*(dbs.at(0)), "INSERT INTO Years (tconst, year) VALUES (?, ?)" };
	    SQLite::Statement runtime_insert {*(dbs.at(0)), "INSERT INTO Runtimes (tconst, runtimeInMin) VALUES (?, ?)" };
	    SQLite::Statement genre_insert {*(dbs.at(0)), "INSERT INTO Genres (tconst, genre) VALUES (?, ?)" };
	    int start = chunksize*(threadnum);
	    int stop = chunksize*(threadnum+1);
	    for (int line = start; line < std::min(stop,size); ++line) {
		std::cerr << threadnum << " : " << (float(line-start)/chunksize)*100 << '\n';
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
		    std::cerr << "Problem reading basics: " << e.what() << '\n';
		    std::cerr << "Rowslicer: " << (*filebuffer).at(line) << '\n';
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
    std::cerr << "Done reading the basics!" << '\n';
}

void loadRatings(std::vector<std::unique_ptr<SQLite::Database>>& dbs, std::ifstream& filestream) {
    /* buffers */
    JTB::Str buf {};

    /* throwing out first line */
    buf.absorbLine(filestream).clear();

    enum Cols { TCONST, RATING, NUMRATES };

    JTB::Vec<std::thread> threadPack {};
    std::mutex mutex {};

    Filebuffer filebuffer { filestream };
    int size = filebuffer.getBuf().size();

    for (int threadnum = 0; threadnum < THREADLIMIT; ++threadnum) {
	threadPack.push([&,threadnum](){
	    SQLite::Statement select1 { *(dbs.at(threadnum)), "SELECT * FROM Films WHERE tconst = ?" };
	    SQLite::Statement select2 { *(dbs.at(threadnum)), "SELECT * FROM Ratings WHERE tconst = ?" };
	    SQLite::Statement insert { *(dbs.at(threadnum)), "INSERT INTO Ratings (tconst, rating, numVotes) VALUES (?, ?, ?)" };
	    int start = filebuffer.getChunksize()*threadnum;
	    int stop = filebuffer.getChunksize()*(threadnum+1);
	    for (int line = start; line < std::min(stop,size); ++line) {
		try {
		    {
			SQLite::Transaction tr {*(dbs.at(threadnum))};
			select1.reset();
			select1.bind(1,filebuffer.getBuf().at(line).at(TCONST).c_str());
			select1.executeStep();
			select2.reset();
			select2.bind(1,filebuffer.getBuf().at(line).at(TCONST).c_str());
			select2.executeStep();
			tr.commit();
		    }
		    if (select1.hasRow() && !select2.hasRow()) {
			std::lock_guard<std::mutex> lock {mutex};
			SQLite::Transaction tr {*(dbs.at(threadnum))};
			insert.reset(); 
			insert.bind(1,filebuffer.getBuf().at(line).at(TCONST).c_str());
			insert.bind(2,std::stof(filebuffer.getBuf().at(line).at(RATING).c_str()));
			insert.bind(3,std::stoi(filebuffer.getBuf().at(line).at(NUMRATES).c_str()));
			insert.exec(); 
			tr.commit();
		    }
		} catch (SQLite::Exception& e) { 
		    std::cerr << "Problem inserting ratings" << '\n';
		    std::cerr << "Problem: " << e.what() << '\n';
		    std::cerr << "Line: " << filebuffer.getBuf().at(line) << '\n';
		} catch (std::exception& e) {
		    std::cerr << "Error inserting ratings" << '\n';
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

    std::cerr << "Done reading ratings!" << '\n';
}

void loadLanguage(std::vector<std::unique_ptr<SQLite::Database>>& dbs, std::ifstream& langfilstream) {
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
	    SQLite::Statement select1 { *(dbs.at(threadnum)), "SELECT * FROM Films WHERE tconst = ?" };
	    SQLite::Statement select2 { *(dbs.at(threadnum)), "SELECT * FROM Languages WHERE tconst = ? AND lang = ?" };
	    SQLite::Statement insert { *(dbs.at(threadnum)), "INSERT INTO Languages (tconst, lang) VALUES (?, ?)" };
	    for (int line = start; line < std::min(stop,size); ++line) {
		if (filebuffer.getBuf().at(line).size() < 2) continue; 
		try { 
		    {
			SQLite::Transaction tr {*(dbs.at(threadnum))};
			select1.reset();
			select1.bind(1,filebuffer.getBuf().at(line).at(TCONST).c_str());
			select1.executeStep();
			select2.reset();
			select2.bind(1,filebuffer.getBuf().at(line).at(TCONST).c_str());
			select2.bind(2,filebuffer.getBuf().at(line).at(LANG).c_str());
			select2.executeStep();
			tr.commit();
		    }
		    if (select1.hasRow() && !select2.hasRow()) {
			std::lock_guard<std::mutex> lock {mutex};
			SQLite::Transaction tr {*(dbs.at(threadnum))};
			insert.bind(1,filebuffer.getBuf().at(line).at(TCONST).c_str());
			insert.bind(2,filebuffer.getBuf().at(line).at(LANG).c_str());
			insert.exec(); 
			insert.reset(); 
			tr.commit();
		    }
		} catch (SQLite::Exception& e) { 
		    std::cerr << "Problem inserting languages" << '\n';
		    std::cerr << "Problem: " << e.what() << '\n';
		    std::cerr << "Row: " << filebuffer.getBuf().at(line) << '\n';
		} catch (std::exception& e) { 
		    std::cerr << "Error inserting languages" << '\n';
		    std::cerr << "Error: " << e.what() << '\n';
		    std::cerr << "Row: " << filebuffer.getBuf().at(line) << '\n';
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


void doIt (std::mutex& mutex, SQLite::Database& db, SQLite::Statement& statement, JTB::Vec<JTB::Str>& rowslicer, int a, int b){
    try {
	std::lock_guard<std::mutex> lock { mutex };
	SQLite::Transaction tr { db };
	statement.reset();
	statement.bind(1, rowslicer.at(a).c_str());
	statement.bind(2, rowslicer.at(b).c_str());
	statement.exec();
	tr.commit();
    } catch (SQLite::Exception& e) {
	std::cerr << "Bonked the retry: "<< e.what() << '\n';
	return;
    }
}

void loadPrincipals(std::vector<std::unique_ptr<SQLite::Database>>& dbs, JTB::Vec<std::ifstream>& principals_streams, std::ifstream& names_stream) {
    /* buffers */
    enum class Names { NCONST, NAME };
    enum class Principles { TCONST, ORDERING, NCONST, CATEGORY, JOB, CHARACTERS };

    /* StreamData name_data { getStreamData(names_stream, THREADLIMIT, [](std::ifstream& ifstream,uint i){ */
	/* JTB::Str buf {}; */
	/* JTB::Vec<JTB::Str> rowslicer {buf.absorbLine(ifstream).split("\t")}; */
	/* if (rowslicer.size() < 2) { */
	    /* return static_cast<uint>(0); */
	/* } */
	/* else { */
	    /* return convertToInt(rowslicer.at(icast(Names::NCONST))); */
	/* } */
    /* }) */ 
    /* }; */
    /* std::unique_ptr<std::vector<JTB::Str*>> namebuf {new std::vector<JTB::Str*>(name_data.linecount+1, nullptr)}; */

    /* { */
	/* JTB::Vec<JTB::Str> rowslicer {}; */
	/* JTB::Str buf {}; */
	/* /1* throwing out first line <== 11/29/24 13:50:55 *1/ */ 
	/* buf.absorbLine(names_stream); */

	/* /1* reading names into map *1/ */
	/* for (uint count = 0; count < name_data.linecount && names_stream.good() && !buf.clear().absorbLine(names_stream).isEmpty();) { */
	    /* rowslicer = buf.split("\t"); */
	    /* if (rowslicer.size() < 2) continue; */ 
	    /* if (convertToInt(rowslicer[static_cast<int>(Names::NCONST)]) < name_data.linecount) { */
		/* (*namebuf)[convertToInt(rowslicer[static_cast<int>(Names::NCONST)])] = new JTB::Str(rowslicer[static_cast<int>(Names::NAME)]); */
		/* ++count; */
	    /* } */
	    /* else { */
		/* std::cerr << convertToInt(rowslicer[static_cast<int>(Names::NCONST)]) << '\n'; */
		/* std::cerr << name_data.linecount << '\n'; */
		/* exit(1); */
	    /* } */
	/* } */
    /* } */
    /* std::cerr << "Done reading names!" << '\n'; */


    /* filling in principals */
    /* Filebuffer filebuffer { principals_stream }; */
    /* int size = filebuffer.getBuf().size(); */
    StreamData principals_data { getStreamData(principals_streams.at(0), THREADLIMIT, [](std::ifstream& is, uint i) {
	JTB::Str buf {};
	try {
	    buf.absorbLine(is);
	    if (is.good()) { 
		return ++i; 
	    }
	    else return i;
	} catch (std::exception& e) {
	    std::cerr << "Error: " << e.what() << '\n';
	    exit(1);
	}
    }) };
    std::cerr << "Done getting principals linecount!" << '\n';

    std::vector<std::thread> threadPack {};
    std::mutex mutex {};
    for (int threadnum = 0; threadnum < std::min(THREADLIMIT,static_cast<int>(principals_data.starts.size())); ++threadnum) {
	threadPack.emplace_back([&,threadnum](){
	    SQLite::Statement wal { *(dbs.at(threadnum)), "pragma journal_mode = WAL" };
	    SQLite::Statement sync { *(dbs.at(threadnum)), "pragma synchronous = normal" };
	    SQLite::Statement tempstore { *(dbs.at(threadnum)), "pragma temp_store = memory" };
	    SQLite::Statement mmap { *(dbs.at(threadnum)), "pragma mmap_size = 30000000000" };
	    SQLite::Statement select1 { *(dbs.at(threadnum)), "SELECT * FROM Films WHERE tconst = ?" };
	    SQLite::Statement select2 { *(dbs.at(threadnum)), "SELECT * FROM Directors WHERE tconst = ? AND nconst = ?" };
	    SQLite::Statement select3 { *(dbs.at(threadnum)), "SELECT * FROM Actors WHERE tconst = ? AND nconst = ?" };
	    SQLite::Statement select4 { *(dbs.at(threadnum)), "SELECT * FROM Writers WHERE tconst = ? AND nconst = ?" };
	    SQLite::Statement directors_insert { *(dbs.at(threadnum)), "INSERT INTO Directors (tconst, nconst) VALUES (?, ?)" };
	    SQLite::Statement actors_insert { *(dbs.at(threadnum)), "INSERT INTO Actors (tconst, nconst) VALUES (?, ?)" };
	    SQLite::Statement writers_insert { *(dbs.at(threadnum)), "INSERT INTO Writers (tconst, nconst) VALUES (?, ?)" };
	    uint seek {};
	    uint stop {};
	    try {
		seek = principals_data.starts.at(threadnum);
		stop = principals_data.stops.at(threadnum);
	    } catch (std::exception& e) {
		std::cerr << "starts and stops read error: " << e.what() << '\n';
		exit(1);
	    }
	    if (principals_streams.at(threadnum).good()) { principals_streams.at(threadnum).seekg(seek); } 
	    else { std::cerr << "Principals stream number " << threadnum << " is not good.\n"; exit(1); }
	    JTB::Str buf {};
	    JTB::Vec<JTB::Str> rowslicer {};
	    uint problemcount {0};
	    if (principals_streams.at(threadnum).good()) { 
		principals_streams.at(threadnum).seekg(seek); 
		if (!principals_streams.at(threadnum).good()) {
		    std::cerr << "Principals stream number " << threadnum << " is not good.\n"; exit(1); 
		}
	    } 
	    else { std::cerr << "Principals stream number " << threadnum << " is not good.\n"; exit(1); }
	    for (; principals_streams.at(threadnum).good() && seek < stop; ++seek) {
		rowslicer = buf.clear().absorbLine(principals_streams.at(threadnum)).split('\t');
		if (rowslicer.size() < 6) continue;
		bool abool = true;
		bool dbool = true;
		bool wbool = true;
		try {
		    {
			std::lock_guard<std::mutex> lock { mutex };
			SQLite::Transaction tr {*(dbs.at(threadnum))};
			select1.reset();
			select1.bind(1, rowslicer.at(icast(Principles::TCONST)).c_str());
			select1.executeStep();
			select2.reset();
			select2.bind(1, rowslicer.at(icast(Principles::TCONST)).c_str());
			select2.bind(2, rowslicer.at(icast(Principles::NCONST)).c_str());
			select2.executeStep();
			dbool = select2.hasRow();
			select3.reset();
			select3.bind(1, rowslicer.at(icast(Principles::TCONST)).c_str());
			select3.bind(2, rowslicer.at(icast(Principles::NCONST)).c_str());
			select3.executeStep();
			abool = select3.hasRow();
			select4.reset();
			select4.bind(1, rowslicer.at(icast(Principles::TCONST)).c_str());
			select4.bind(2, rowslicer.at(icast(Principles::NCONST)).c_str());
			select4.executeStep();
			wbool = select4.hasRow();
			tr.commit();
		    }
		    if (!select1.hasRow()) continue;
		    /* actors <== 11/29/24 15:39:28 */ 
		    if (rowslicer.at(icast(Principles::CATEGORY)).startsWith("a") && !abool) {
			try {
			    doIt(mutex,*(dbs.at(threadnum)),actors_insert,rowslicer,icast(Principles::TCONST),icast(Principles::NCONST));
			} catch (SQLite::Exception& e) {
			    std::cerr << "actor error: " << e.what() << '\n';
			    JTB::Str error {e.what()};
			    if (error.includes("lock")) {
				std::this_thread::sleep_for(std::chrono::milliseconds(300*(threadnum+1)));
				doIt(mutex,*(dbs.at(threadnum)),actors_insert,rowslicer,icast(Principles::TCONST),icast(Principles::NCONST));
			    }
			}
		    }
		    /* directors <== 11/29/24 15:39:32 */ 
		    else if (rowslicer.at(icast(Principles::CATEGORY)).startsWith("d") && !dbool) {
			try {
			    doIt(mutex,*(dbs.at(threadnum)),directors_insert,rowslicer,icast(Principles::TCONST),icast(Principles::NCONST));
			} catch (SQLite::Exception& e) {
			    std::cerr << "director error: " << e.what() << '\n';
			    JTB::Str error {e.what()};
			    if (error.includes("lock")) {
				std::this_thread::sleep_for(std::chrono::milliseconds(220*(threadnum+1)));
				doIt(mutex,*(dbs.at(threadnum)),directors_insert,rowslicer,icast(Principles::TCONST),icast(Principles::NCONST));
			    }
			}
		    }
		    /* writers <== 11/29/24 15:39:37 */ 
		    else if (rowslicer.at(icast(Principles::CATEGORY)).startsWith("w") && !wbool) {
			try {
			    doIt(mutex,*(dbs.at(threadnum)),writers_insert,rowslicer,icast(Principles::TCONST),icast(Principles::NCONST));
			} catch (SQLite::Exception& e) {
			    std::cerr << "writer error: " << e.what() << '\n';
			    JTB::Str error {e.what()};
			    if (error.includes("lock")) {
				std::this_thread::sleep_for(std::chrono::milliseconds(140*(threadnum+1)));
				doIt(mutex,*(dbs.at(threadnum)),writers_insert,rowslicer,icast(Principles::TCONST),icast(Principles::NCONST));
			    }
			}
		    }
		} catch (SQLite::Exception& e) {
		    std::cerr << ++problemcount << "Problem with principals: " << e.what() << '\n';
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
    
    std::cerr << "Done reading principals!" << '\n';
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
    JTB::Vec<std::ifstream> principals_streams(THREADLIMIT);
    std::ifstream name_basics_stream {};
    try { 
	lang_stream.open( movieDatabasePath.str() + "/lang.tsv" );
	basics_stream.open( movieDatabasePath.str() + "/title.basics.tsv" ); 
	ratings_stream.open( movieDatabasePath.str() + "/title.ratings.tsv" ); 
	principals_streams.forEach([&](std::ifstream& istream) {
	    istream.open( movieDatabasePath.str() + "/title.principals.tsv" );
	});
	name_basics_stream.open( movieDatabasePath.str() + "/name.basics.tsv" );
    } catch (std::exception& e) { 
	std::cerr << "Problem with opening filestreams" << '\n';
	std::cerr << "Error: " << e.what() << '\n';
	exit(1);
    }

    try {
	std::vector<std::unique_ptr<SQLite::Database>> dbs {};
	for (int i = 0; i < THREADLIMIT; ++i) {
	    SQLite::Database* db 
		{new SQLite::Database {movieDatabasePath.str() + "/moviedatabase.db", SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE}};
	    dbs.emplace_back(db);
	};	
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Films" (
	    tconst TEXT NOT NULL PRIMARY KEY,
	    title TEXT NOT NULL,
	    originalTitle TEXT NOT NULL))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Genres" (
	    tconst TEXT NOT NULL REFERENCES Films(tconst),
	    genre TEXT NOT NULL))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Runtimes" (
	    tconst TEXT NOT NULL REFERENCES Films(tconst),
	    runtimeInMin INT NOT NULL,
	    UNIQUE(tconst, runtimeInMin)))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Years" (
	    tconst TEXT NOT NULL REFERENCES Films(tconst),
	    year INT NOT NULL,
	    UNIQUE(tconst, year)))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Names" (
	    nconst TEXT NOT NULL PRIMARY KEY,
	    name TEXT NOT NULL,
	    UNIQUE(nconst, name)))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Directors" (
	    tconst TEXT NOT NULL REFERENCES Films(tconst),
	    nconst TEXT NOT NULL REFERENCES Names(nconst),
	    UNIQUE(tconst,nconst)))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Actors" (
	    tconst TEXT NOT NULL REFERENCES Films(tconst),
	    nconst TEXT NOT NULL REFERENCES Names(nconst),
	    UNIQUE(tconst,nconst)))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Writers" (
	    tconst TEXT NOT NULL REFERENCES Films(tconst),
	    nconst TEXT NOT NULL REFERENCES Names(nconst),
	    UNIQUE(tconst,nconst)))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "KnownFor" (
	    tconst TEXT NOT NULL REFERENCES Films(tconst),
	    nconst TEXT NOT NULL REFERENCES Names(nconst),
	    UNIQUE(tconst,nconst)))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Ratings" (
	    tconst TEXT NOT NULL UNIQUE REFERENCES Films(tconst),
	    rating FLOAT NOT NULL,
	    numVotes INTEGER NOT NULL))");
	dbs.at(0)->exec(R"(CREATE TABLE IF NOT EXISTS "Languages" (
	    tconst TEXT NOT NULL REFERENCES Films(tconst),
	    lang TEXT NOT NULL))");

	/* loadBasics(dbs, basics_stream); */
	/* loadRatings(dbs, ratings_stream); */
	/* loadLanguage(dbs, lang_stream); */
	loadPrincipals(dbs, principals_streams, name_basics_stream);
    } catch (std::exception& e) {
	std::cerr << e.what() << '\n';
	    exit(1);
    }

    std::cout << "All done!" << '\n';
}
