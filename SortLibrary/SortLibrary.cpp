#include "pch.h"
#include "SortLibrary.h"
#include "FileManagementLibrary.h"
#include <windows.h>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <iostream>
using std::map;
using std::cerr;
using std::string;
using std::vector;
using std::ifstream;
using std::istringstream;

typedef int (*funcReadAllDirectoryFileContents)(const string&);

// Factory function to create a Sort class
PSortLibrary* createSort()
{
    return new SortLibrary();
}

map <string, vector<int>> SortLibrary::create_word_map(string tempDir)
{

    // Create a variable that will be used to look at the current line
    string line;
    // Create a variable that will be used as the current part being examined
    // when line is divided into two parts at the comma
    string part;
    // Create a variable that will store a word without any symbols
    string strNew;
    // Create a variable that will hold the value in the tuple without any symbols
    int value;
    // Create variable that will be used to loop through parts
    int count = 0;

    // Create a map of strings and integers that will hold the words and their respective values
    map< string, vector<int>> words;

    //load File Management DLL
    HINSTANCE hDLL;
    funcReadAllDirectoryFileContents readAllDirectoryFileContents;
    const wchar_t* libName = L"FileManagementLibrary";
    hDLL = LoadLibraryEx(libName, NULL, NULL);
    //if File Management DLL is loaded, load the readAllDirectoryFileContents function from DLL and continue with program
    if (hDLL != NULL) {
        //load the readAllDirectoryFileContents function from the DLL
        readAllDirectoryFileContents = (funcReadAllDirectoryFileContents)GetProcAddress(hDLL, "readAllDirectoryFileContents");
        // if the function is loaded, call the function to read all the files in the directory and continue with the program
        if (readAllDirectoryFileContents != NULL) {
            readAllDirectoryFileContents(tempDir);
            // Create a variable that will be used as the input file
            ifstream input_file(tempDir + "\\allWords.txt");
            // Get line in file
            while (getline(input_file, line))
            {
                istringstream is(line);

                // Separate word from integer/count
                while (getline(is, part, ','))
                {
                    // If first loop, remove quotation
                    if (count == 0) {
                        unsigned first = part.find("\"");
                        unsigned last = part.find_last_of("\"");
                        strNew = part.substr(first, last - first);
                        if (strNew.front() == '"') {
                            strNew.erase(0, 1); // erase the first quotation
                        }
                    }
                    // If second iteration, remove parenthesis, get integer value and add word and count to map
                    else if (count == 1)
                    {
                        if (part.back() == ')')
                        {
                            // Get rid of the closing parenthesis
                            part.erase(part.size() - 1);
                        }
                        // Convert to int
                        value = stoi(part);

                        // Since word and int are acquired, store in map
                        if (words.find(strNew) == words.end())
                        {
                            // Not found, intialize vector
                            vector <int> nums = { value };
                            words[strNew] = nums;

                        }
                        else
                        {
                            // Found and store new int in vector
                            words[strNew].push_back(value);
                        }
                        // Reset counter and break
                        count = 0;
                        break;
                    }
                    // Add to count after trimming string and assigning word to strNew
                    count++;
                }
            }
        }
        else {
            cerr << "Could not load the function to read all of the temp files/n";
        }
        //free the library
        FreeLibrary(hDLL);

    }
    else {
        cerr << "Could not load the dynamic library\n";
    }

    // Return the map
    return words;
}