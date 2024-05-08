#pragma once

#ifdef MAPLIBRARY_EXPORTS
#define MAPLIBRARY_API __declspec(dllexport)
#else
#define MAPLIBRARY_API __declspec(dllimport)
#endif

#include <vector>
#include <string>
#include <unordered_map>
#define DllExport   __declspec( dllexport )
using std::string;
using std::unordered_map;

//abstract parent class for Map class
class PMapLibrary {
public:
    virtual void map(const string& key, const string& value) {};

};

// Actual Map class is only used internally and is not exposed
class MapLibrary : public PMapLibrary
{
public:
    // Constructor to initialize the Map with a directory for temporary storage
    MapLibrary(string tempDir) : tempDirectory(tempDir) {}  // Initializes the tempDirectory member with the provided directory path

    // Process a key-value pair by tokenizing and counting words
    void map(const string& key, const string& value);

    // Export the current word counts to a file
    void exportToFile();

private:
    unordered_map<string, int> wordCount; // Hash table to store word counts
    void flushBuffer(); // Flush remaining words from buffer to file
    int bufferCount = 0; // Buffer Tracker
    const int bufferThreshold = 500000; // Threshold for the buffer
    string fileName; // Name of the temp file where word counts are stored

    const string tempDirectory; // Directory path for storing temp files
};

// Factory function to create a Map class and return a pointer
extern "C" MAPLIBRARY_API PMapLibrary * createMap(string inputTemp);