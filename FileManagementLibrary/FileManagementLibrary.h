#pragma once
#include <iostream>
using std::string;
// FileManagement.h 
#pragma once
#ifdef FILEMANAGEMENTLIBRARY_EXPORTS
#define FILEMANAGEMENTLIBRARY_API __declspec(dllexport)
#else
#define FILEMANAGEMENTLIBRARY_API __declspec(dllimport)
#endif

// Function that will create a directory, given a directory path as dirPath
extern "C" FILEMANAGEMENTLIBRARY_API int createDirectory(const string & dirPath);
// Function that will check if a directory exists, given a directory path as dirPath
extern "C" FILEMANAGEMENTLIBRARY_API int isDirectoryPresent(const string & dirPath);
// Function that will check if a directory is empty, given a directory path as dirPath
extern "C" FILEMANAGEMENTLIBRARY_API int isDirectoryEmpty(const string & dirPath);

// Function that will read all file contents from a directory, given a directory path as dirPath
extern "C" FILEMANAGEMENTLIBRARY_API int readAllDirectoryFileContents(const string & dirPath);
// Function that will delete all file contents from a directory, given a directory path as dirPath
extern "C" FILEMANAGEMENTLIBRARY_API int deleteDirectoryContents(const string & dirPath);

// Function that will create a single file, given a file path as filePath
extern "C" FILEMANAGEMENTLIBRARY_API int createFile(const string & filePath);
// Function that will delete a single file, given a file path as filePath
extern "C" FILEMANAGEMENTLIBRARY_API int deleteFile(const string & filePath);
// Function that will read and output the data from a file as a string
extern "C" FILEMANAGEMENTLIBRARY_API string readDatafromFile(const string & filePath);
// Function that will write to a file, given a file path as filePath and input as data
extern "C" FILEMANAGEMENTLIBRARY_API int writeDataToFile(const string & filePath, const string & data);