#pragma once
#ifdef REDUCELIBRARY_EXPORTS
#define REDUCELIBRARY_API __declspec(dllexport)
#else
#define REDUCELIBRARY_API __declspec(dllimport)
#endif

#include <iostream>
#include <vector>
#define DllExport   __declspec( dllexport )
using std::string;
using std::vector;

//abstract parent class for the reduce class
class PReduceLibrary {
public:
	//virtual reduce function
	virtual int reduce(string key, vector<int> count) { return 1; };
};

class ReduceLibrary : public PReduceLibrary
{
public:
	// Constructor that takes in the directory that the export file
	// should be in and saves it as a class parameter, theDir
	ReduceLibrary(string theDirInput)
		:theDir(theDirInput) {};

	// Function that takes in the string key and vector of intergers, sums the vector,
	// and passes the sum and key to the export function to output it to a file
	int reduce(string keyInput, vector<int> countInput);

private:
	// Export function called by reduce function that accepts the key and vector sum,
	// it creates a file in the theDir directory if it does not exist and adds the key and interger to the file
	int exportReduce(string key, int reducedValue);

	// Variable to hold the directory that is used in the exportReduce function
	string theDir;

};

//factory function to pass a pointer to a reduce class
extern "C" REDUCELIBRARY_API PReduceLibrary * createReduce(string theDirInput);