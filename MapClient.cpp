/*
Chandler Nunokawa
Dana Marie Castillo Chea
James Lu
Sarah Welvaert

Professor Scott Roueche
CSE-687 Object Oriented Design

Syracuse University
Project Phase 2
05/08/2024
This is the driver class for MapReduce. The program will take an input directory where text files
are stored and will ultimately produce a single output file that contains a list of words and
their associated counts in the originating input files.*/

#include "MapLibrary.h"
#include "FileManagementLibrary.h"
#include "ReduceLibrary.h"
#include "SortLibrary.h"
#include <windows.h>
#include <iostream>
#include <filesystem>
#include <thread>
#include <mutex>
#define DllImport   __declspec( dllimport )


using std::string;
using std::map;
using std::vector;
using std::cout;
using std::cin;
using std::cerr;
using std::thread;

//creates typedefs for the file management functions used in this file
typedef int (*funcIsDirectoryPresent)(const string& dirPath);
typedef int (*funcIsDirectoryEmpty)(const string& dirPath);
typedef int (*funcDeleteDirectoryContents)(const string& dirPath);
typedef string(*funcReadDatafromFile)(const string& filePath);
typedef int (*funcCreateFile)(const string& filePath);
typedef PMapLibrary* (*Map_Factory)(const string&);
typedef PReduceLibrary* (*Reduce_Factory)(const string&);
typedef PSortLibrary* (*Sort_Factory)();
std::mutex g_num_mutex;

void f1(PMapLibrary *pMap, string dirPath, string fileContent)
{
    g_num_mutex.lock();
    cout << "f1 called\n";
    pMap->map(dirPath, fileContent);
    g_num_mutex.unlock();
}

int main(int argc, char* argv[])
{
    // Load the DLLs
    HINSTANCE fileDLL;
    HINSTANCE mapDLL;
    HINSTANCE reduceDLL;
    HINSTANCE sortDLL;
    const wchar_t* filelibName = L"FileManagementLibrary";
    const wchar_t* maplibName = L"MapLibrary";
    const wchar_t* reducelibName = L"ReduceLibrary";
    const wchar_t* sortlibName = L"SortLibrary";

    fileDLL = LoadLibraryEx(filelibName, NULL, NULL);
    mapDLL = LoadLibraryEx(maplibName, NULL, NULL);
    reduceDLL = LoadLibraryEx(reducelibName, NULL, NULL);
    sortDLL = LoadLibraryEx(sortlibName, NULL, NULL);

    // Program banner
    cout << "********** MapReduce Application **********\n\n";

    // For this program, the input directory name is "inputs"
    string inputDir;
    // For this program, the output directory name is "outputs"
    string outputDir;
    string outputDir1;
    // For this program, the temp directory name is "temps"
    string tempDir;
    string tempDir1;

    if (fileDLL != NULL && mapDLL != NULL && reduceDLL != NULL && sortDLL != NULL) {
        //loads the map class factory function from the DLL
        Map_Factory mapFactory = (Map_Factory)GetProcAddress(mapDLL, "createMap");
        if (!mapFactory) {
            cerr << "Error: Unable to find map factory\n";
            return 1;
        }

        //loads the reduce class factory function from the DLL
        auto reduceFactory = reinterpret_cast<Reduce_Factory>(GetProcAddress(reduceDLL, "createReduce"));
        if (!reduceFactory) {
            cerr << "Error: Unable to find reduce factory\n";
            return 1;
        }

        //loads the reduce class factory function from the DLL
        auto sortFactory = reinterpret_cast<Sort_Factory>(GetProcAddress(sortDLL, "createSort"));
        if (!sortFactory) {
            cerr << "Error: Unable to find sort factory\n";
            return 1;
        }

        //loads file management functions from the DLL used in this file
        funcIsDirectoryPresent isDirectoryPresent = (funcIsDirectoryPresent)GetProcAddress(fileDLL, "isDirectoryPresent");
        funcIsDirectoryEmpty isDirectoryEmpty = (funcIsDirectoryEmpty)GetProcAddress(fileDLL, "isDirectoryEmpty");
        funcDeleteDirectoryContents deleteDirectoryContents = (funcDeleteDirectoryContents)GetProcAddress(fileDLL, "deleteDirectoryContents");
        funcReadDatafromFile readDatafromFile = (funcReadDatafromFile)GetProcAddress(fileDLL, "readDatafromFile");
        funcCreateFile createFile = (funcCreateFile)GetProcAddress(fileDLL, "createFile");

        //if the file management functions are found proceed with program
        if (isDirectoryPresent != NULL && isDirectoryEmpty != NULL && deleteDirectoryContents != NULL && readDatafromFile != NULL && createFile != NULL) {

            // Convert command-line arguments to appropriate types
            // Prompt user for input directory
            int inputDirectory = 1;
            while (inputDirectory)
            {
                cout << "Please type in a valid input directory.\n";
                cin >> inputDir;

                inputDirectory = isDirectoryPresent(inputDir);

                // If input directory exists, but empty, then continue to prompt user for valid input directory
                if (inputDirectory == 0)
                {
                    if (isDirectoryEmpty(inputDir))
                    {
                        cout << "This directory is empty.\n";
                        inputDirectory = 1;
                    }
                }
            }

            // Prompt user for output directory
            int outputDirectory = 1;
            while (outputDirectory)
            {
                cout << "Please type in a valid output directory.\n";
                cin >> outputDir;
                outputDirectory = isDirectoryPresent(outputDir);
            }
            outputDir1 = outputDir;

            // Prompt user for temporary directory
            int tempDirectory = 1;
            while (tempDirectory)
            {
                cout << "Please type in a valid temp directory.\n";
                cin >> tempDir;
                tempDirectory = isDirectoryPresent(tempDir);
            }
            tempDir1 = "temps";
            cout << "this is tempDir11 " << tempDir1 << "\n";
            // Check with the user if the output and temp directories can be cleared
            int userCheck = 1;
            cout << "To run the program correctly, the output and temp directories will be emptied. Is this okay? (0: no, 1: yes)\n";
            cin >> userCheck;

            // If the user says that the directories can not be emptied, end the program and inform the user that the program is unable to continue
            if (userCheck == 0)
            {
                cout << "As you do not want the program to empty the temp and output directories, the program is unable to continue.\n";
                return 0;
            }
            else
            {
                // Ensure that the temporary directory is empty
                cout << "this is tempDir12 " << tempDir1 << "\n";

                deleteDirectoryContents(tempDir);
                cout << "this is tempDir13 " << tempDir1 << "\n";

                // Ensure that the output directory is empty
                deleteDirectoryContents(outputDir);
            }

            cout << "this is tempDir14 " << tempDir1 << "\n";

            // Iterate through the input files in the input directory
            for (const auto& entry : std::filesystem::directory_iterator(inputDir))
            {
                    // Use the map function to pass in the file name and the file contents
                    cout << "path name: " << entry.path().filename().string() << "\n";
                    cout << "this is tempDir15 " << tempDir1 << "\n";

                    if (entry.path().filename().string() == "AMidSummerNightsDream.txt")
                    {
                        cout << "this is tempDir16 " << tempDir1 << "\n";

                        auto pMap = mapFactory(tempDir1);
                        cout << "this is the tempDir1-1: " << tempDir1 << "\n";
                        // Read each file and output its contents
                        string fileContent = readDatafromFile(entry.path().string());
                        thread thr(f1, pMap, entry.path().filename().string(), fileContent);
                        cout << "thread id in loop: " << thr.get_id() << "\n";
                        thr.join();
                        delete pMap;
                    }
                    else
                    {
                        cout << "this is tempDir17 " << tempDir1 << "\n";

                        auto pMap = mapFactory(tempDir1);
                        cout << "this is the tempDir1-2: " << tempDir1 << "\n";
                        // Read each file and output its contents
                        string fileContent = readDatafromFile(entry.path().string());
                        thread thr1(f1, pMap, entry.path().filename().string(), fileContent);
                        cout << "thread id in loop: " << thr1.get_id() << "\n";
                        cout << "this is tempDir21 " << tempDir1 << "\n";
                        thr1.join();
                        cout << "this is tempDir20 " << tempDir1 << "\n";
                        delete pMap;
                        cout << "this is tempDir19 " << tempDir1 << "\n";
                    }
                    //release memory used by map
                    cout << "this is tempDir18 " << tempDir1 << "\n";
            }

            //// Create a map class, taking in the temporary directory as a parameter

            //auto pMap = mapFactory(tempDir1);

            ////if the map class is not created, inform the user
            //if (!pMap) {
            //    cerr << "Error: map factory failed\n";
            //    return 1;
            //}

            //// Iterate through the input files in the input directory
            //for (const auto& entry : std::filesystem::directory_iterator(inputDir))
            //{
            //    // Read each file and output its contents
            //    string fileContent = readDatafromFile(entry.path().string());

            //    // Use the map function to pass in the file name and the file contents
            //    pMap->map(entry.path().filename().string(), fileContent);
            //}
            ////release memory used by map class
            //delete pMap;

            // Create a Sort class
            auto pSort = sortFactory();

            //if the sort class is not created, inform the user
            if (!pSort) {
                cerr << "Error: sort factory failed\n";
                return 1;

            }

            // Call the create_word_map function, which goes through the temp directory and returns a map
            // with all the words in the temp directory files and a vector of of the numbers associated with the word
            map <string, vector<int>> words = pSort->create_word_map(tempDir);

            delete pSort;

            // Create a varible to determine if all of the words have been added to the file correctly,
            // if they have it will remain 0
            int isSuccessful = 0;

            // Creates a Reduce class that saves the output directory
            auto pReduce = reduceFactory(outputDir);
            //if the reduce class is not created, inform the user
            if (!pReduce) {
                cerr << "Error: reduce factory failed\n";
                return 1;
            }

            // Loop to run through the string vector pairs in the map and use a reduce class
            // to add the word and the vector sum to an output file in the output directory
            for (const auto& pair : words)
            {
                // Call the reduce function that adds together the vector to create a vector sum
                // and outputs the key and the sum to a file in the output directory
                // Reduce function returns 0 if it is successful at adding it and returns 1 if unsuccessful,
                // that number is added to isSuccessful
                isSuccessful = isSuccessful + pReduce->reduce(pair.first, pair.second);

            }
            // release reduce class memory
            delete pReduce;

            // If the previous loop was able to add all of the key, sum pairs to the file
            // an success file is created in the output directory
            if (isSuccessful == 0) {
                int createOutput = createFile(outputDir1 + "\\Success.txt");
            }

            // Program is complete
            cout << "\nProgram complete.\n";
        }
        //inform user if the file management functions are not found
        else {
            cerr << "Error: Function not found in one of the DLLs.\n";
        }

        //free the libraries loabed
        FreeLibrary(fileDLL);
        FreeLibrary(mapDLL);
        FreeLibrary(reduceDLL);
        FreeLibrary(sortDLL);
    }
    //if the DLLs don't load inform user
    else {
        cerr << "Error: Failed to load one or more DLLs.\n";
    }
}