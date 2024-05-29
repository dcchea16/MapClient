#include "pch.h"
#include "ReduceLibrary.h"
#include "FileManagementLibrary.h"
#include <windows.h>
#include <iostream>
#include <filesystem>
#include <string>

typedef double (*funcCreateFile)(const string&);
typedef double (*funcWriteToFile)(const string&, const string&);
// ReduceLibrary function that accepts a string and vector of intergers and passes the string and the vector sum to the export function,
// returns 0 if it is able to add it to the file, 1 if it encounters an error
int ReduceLibrary::reduce(string keyInput, vector<int> countInput)
{
    // Create a variable to hold the sum of the vector
    int reducedValue = 0;
    // Loop to add the intergers from the vector together
    for (int aCount : countInput) {
        reducedValue = reducedValue + aCount;
    }
    // Call the exportReduce function to add the key and the sum to a file
    // Return 0 if it is abble to add it to the file, returns 1 if it encounters an error
    return exportReduce(keyInput, reducedValue);
}

void ReduceLibrary::setOutputDirectory(string inputDir)
{
    theDir = inputDir;
}

// Export function that takes in the key and the sum and outputs it to a theOutput.txt file in the specified directory
int ReduceLibrary::exportReduce(string key, int reducedValue) {
    int added = 1;
    HINSTANCE hDLL;
    funcCreateFile createFile;
    funcWriteToFile writeDataToFile;
    const wchar_t* libName = L"FileManagementLibrary";
    hDLL = LoadLibraryEx(libName, NULL, NULL); // Handle to DLL
    if (hDLL != NULL) {
        createFile = (funcCreateFile)GetProcAddress(hDLL, "createFile");
        writeDataToFile = (funcWriteToFile)GetProcAddress(hDLL, "writeDataToFile");
        // Create the file name using the directory path and the name of the file
        string outputFileName = theDir + "\\theOutput.txt";

        // If the file exists don't need to do anything
        if (std::filesystem::exists(outputFileName))
        {
        }
        // If the file doesn't exist, create it
        else
        {
            if (createFile != NULL) {
                createFile(outputFileName);
            }
        }

        // Create a single string with the key and the sum in the format they should be added to the file
        string key_value_pair = "(\"" + key + "\"," + std::to_string(reducedValue) + ")\n";
        // Call a FileManagement function to write the string to the file, the function will return 0 if successful and 1 if there is an error
        if (writeDataToFile != NULL) {
            added = writeDataToFile(outputFileName, key_value_pair);
        }

        // Returns the result of the FileManagement function
        FreeLibrary(hDLL);
    }
    else {
        std::cout << "Library load failed!" << std::endl;
    }
    return added;
}

REDUCELIBRARY_API PReduceLibrary* createReduce()
{
    return new ReduceLibrary();
}