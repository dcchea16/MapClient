#include "pch.h"  
#include <utility>
#include <limits.h>
#include "MapLibrary.h"
#include <filesystem>
#include "FileManagementLibrary.h"
#include <boost/algorithm/string.hpp> 
#include <iostream>
#include <windows.h>
using std::cerr;
using std::cout;
using std::vector;
using std::to_string;

typedef int (*funcCreateFile)(const string&);
typedef int (*funcWriteToFile)(const string&, const string&);


// Factory function to create a Map class and return a pointer
PMapLibrary* createMap()
{
    return new MapLibrary();
}

void MapLibrary::setTempDirectory(const string& inputDir) {
    tempDirectory = inputDir;
}

// Processes the input key-value pair to tokenize and count word occurrences
void MapLibrary::map(const string& key, const string& value)
{
    HINSTANCE hDLL;
    funcCreateFile createFile;
    funcWriteToFile writeDataToFile;
    const wchar_t* libName = L"FileManagementLibrary";
    hDLL = LoadLibraryEx(libName, NULL, NULL); // Handle to DLL
    if (hDLL != NULL) {
        writeDataToFile = (funcWriteToFile)GetProcAddress(hDLL, "writeDataToFile");
        createFile = (funcCreateFile)GetProcAddress(hDLL, "createFile");

        if (writeDataToFile != NULL) {
            // Stores the tokens after splitting the value
            vector<string> tokens;
            // Constructs the file name from the key
            fileName = tempDirectory + "\\" + key;
            // Attempts to create a file for the output
            createFile(fileName);

            // Delimiters used for splitting the value into tokens
            const char* delimeters = "-[]\n\t ,.!?;:&\"'";
            // Splits the value into tokens based on delimiters
            boost::algorithm::split(tokens, value, boost::algorithm::is_any_of(delimeters), boost::algorithm::token_compress_on);
            // Logging the export operation
            cout << "Exporting file: " + key + '\n';

            // Iterate through all the tokenized strings
            for (auto& token : tokens)
            {
                // Converts each token to lower case for uniformity
                boost::algorithm::to_lower(token);

                // Check if the token is non-empty
                if (!token.empty())
                {
                    // Increment the count for the word
                    wordCount[token]++;

                    // Increment the buffer count
                    bufferCount++;
                    // Check if the buffer threshold is exceeded
                    if (bufferCount >= bufferThreshold)
                    {
                        // Export the current counts to file
                        exportToFile();
                        // Clear the word count map
                        wordCount.clear();
                        // Reset the buffer count
                        bufferCount = 0;
                    }
                }
            }
            // Ensure all remaining data is written to the file
            flushBuffer();
        }
    }
    else {
        cerr << "Library load failed!\n";
    }
    //free library
    FreeLibrary(hDLL);
}

// Flushes the remaining word counts to the file if any
void MapLibrary::flushBuffer()
{
    // Check if there are any remaining word counts
    if (!wordCount.empty())
    {
        // Export them to the file
        exportToFile();
        // Clear the word count map
        wordCount.clear();
        // Reset the buffer count
        bufferCount = 0;
    }
}

void MapLibrary::exportToFile() {
    HINSTANCE hDLL;
    funcCreateFile createFile;
    funcWriteToFile writeDataToFile;
    const wchar_t* libName = L"FileManagementLibrary";
    hDLL = LoadLibraryEx(libName, NULL, NULL); // Handle to DLL
    if (hDLL != NULL) {
        writeDataToFile = (funcWriteToFile)GetProcAddress(hDLL, "writeDataToFile");

        if (writeDataToFile != NULL) {
            // Iterate through all the word count pairs
            for (const auto& wordCountPair : wordCount)
            {
                // Format each word count as a tuple string
                string wordCombo = "(\"" + wordCountPair.first + "\", " + to_string(wordCountPair.second) + ")\n";
                // Write the word count tuple to the file
                writeDataToFile(fileName, wordCombo);
            }
        }
        else {
            cerr << "Function writeDataToFile not found in DLL\n";
        }

        // Returns the result of the FileManagement function
        FreeLibrary(hDLL);
    }
    else {
        cout << "Library load failed!\n";
    }
}