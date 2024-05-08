/*
This is the header file for the Sort class. This will contain all of the function declarations
for the program and its private members. The Sort class will look at the intermediate results to sort and aggregate them.
Then an intermediate file will be generated.
*/
#pragma once

#ifdef SORTLIBRARY_EXPORTS
#define SORTLIBRARY_API __declspec(dllexport)
#else
#define SORTLIBRARY_API __declspec(dllimport)
#endif

#include <string>
#include <map>
#include <vector>
#define DllExport   __declspec( dllexport )
using std::map;
using std::string;
using std::vector;

//abstract parent class for Sort class
class PSortLibrary {
public:
	virtual map <string, vector<int>> create_word_map(string tempDir) { std::map < string, vector<int>> mapTest; return mapTest; };
};

class SortLibrary : public PSortLibrary
{
public:

	// Default constructor
	SortLibrary() {};

	// Destructor
	~SortLibrary() {};

	// This function will create a word map that will look at the intermediate results
	// to sort and aggregate them, given the tempDir
	map <string, vector<int>> create_word_map(string tempDir);
};

// Factory function to create a Sort class
extern "C" SORTLIBRARY_API PSortLibrary * createSort();