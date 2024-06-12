#include "pch.h"
#include "framework.h"
#include "FileManagementLibrary.h"
#include <iostream>
#include <filesystem>
#include <string>
#include <fstream>
#include <streambuf>
using std::string;
using std::cerr;
using std::ofstream;
using std::ifstream;
using std::istreambuf_iterator;

// Function that will create a directory, given a directory path as dirPath
FILEMANAGEMENTLIBRARY_API int createDirectory(const string& dirPath)
{
	// Check if the directory path already exists
	if (std::filesystem::exists(dirPath))
	{
		cerr << "Error directory path (" << dirPath << ") already exists!\n";
		return 1;
	}

	std::filesystem::create_directory(dirPath);
	return 0;
}

// Function that will check if a directory exists, given a directory path as dirPath
FILEMANAGEMENTLIBRARY_API int isDirectoryPresent(const string& dirPath)
{
	// Check if the directory path exists
	if (!std::filesystem::exists(dirPath))
	{
		return 1;
	}

	return 0;
}

// Function that will check if a directory is empty, given a directory path as dirPath
FILEMANAGEMENTLIBRARY_API int isDirectoryEmpty(const string& dirPath)
{
	// Check if the directory path exists
	if (std::filesystem::is_empty(dirPath))
	{
		return 1;
	}

	return 0;
}

// Function that will read all file contents from a directory, given a directory path as dirPath
FILEMANAGEMENTLIBRARY_API int readAllDirectoryFileContents(const string& dirPath)
{
	// Check if the directory path exists
	if (!std::filesystem::exists(dirPath))
	{
		cerr << "error directory path (" << dirPath << ") doesn't exist!\n";
		return 1;
	}
	// Check if the directory path is empty
	if (std::filesystem::is_empty(dirPath))
	{
		cerr << "directory path (" << dirPath << ") is empty!\n";
		return 1;
	}

	// Create a file that will hold all of the file contents in the given directory
	ofstream outputFile;
	createFile(dirPath + "\\allWords.txt");

	// Iterate through all files in the given directory
	for (const auto& entry : std::filesystem::directory_iterator(dirPath))
	{
		//Skip the allWords.txt file in order to not double count
		string fileName = entry.path().filename().string();
		if (fileName == "allWords.txt") {
			continue;
		}
		// Read data from each file
		string contents = readDatafromFile(entry.path().string());
		// Write data from each file to the single output file
		writeDataToFile(dirPath + "\\allWords.txt", contents);
	}

	return 0;
}

// Function that will delete all file contents from a directory, given a directory path as dirPath
FILEMANAGEMENTLIBRARY_API int deleteDirectoryContents(const string& dirPath)
{
	// Check if the directory path exists
	if (!std::filesystem::exists(dirPath))
	{
		cerr << "Error directory path (" << dirPath << ") doesn't exist!\n";
		return 1;
	}
	// Check if the directory path empty
	if (std::filesystem::is_empty(dirPath))
	{
		return 1;
	}

	// Iterate through all the directory files and delete them
	for (const auto& entry : std::filesystem::directory_iterator(dirPath))
	{
		std::filesystem::remove_all(entry.path());
	}

	return 0;
}

// Function that will create a single file, given a file path as filePath
FILEMANAGEMENTLIBRARY_API int createFile(const string& filePath)
{
	// Check if the file path exists
	if (std::filesystem::exists(filePath))
	{
		cerr << "Error file path (" << filePath << ") already exists!\n";
		return 1;
	}

	// Create the file with the name given by filePath
	std::ofstream ofs(filePath);
	ofs.close();
	return 0;
}

// Function that will delete a single file, given a file path as filePath
FILEMANAGEMENTLIBRARY_API int deleteFile(const string& filePath)
{
	// Check if the file path exists
	if (!std::filesystem::exists(filePath))
	{
		cerr << "Error file path (" << filePath << ") does not exist\n";
		return 1;
	}

	// Remove file with the name given by filePath
	std::filesystem::remove(filePath);
	return 0;
}

// Function that will read and output the data from a file as a string
FILEMANAGEMENTLIBRARY_API string readDatafromFile(const string& filePath)
{
	// Check if the file path exists
	if (!std::filesystem::exists(filePath))
	{
		cerr << "Error file path (" << filePath << ") not found!\n";
		return string();
	}
	// Check if the file path is empty
	else if (std::filesystem::is_empty(filePath))
	{
		cerr << "Error file path (" << filePath << ") is empty!\n";
		return string();
	}

	// Read the file's contents and return as a string
	ifstream t(filePath);
	return string(istreambuf_iterator<char>(t), istreambuf_iterator<char>());
}

// Function that will write to a file, given a file path as filePath and input as data
FILEMANAGEMENTLIBRARY_API int writeDataToFile(const string& filePath, const string& data)
{
	// Check if the file path exists
	if (!std::filesystem::exists(filePath))
	{
		cerr << "Error file path (" << filePath << ") not found!\n";
		return 1;
	}

	// Write data to the file, given by the filePath and the data provided
	ofstream ofs(filePath, std::ios_base::app);
	ofs << data;
	ofs.close();
	return 0;
}

FILEMANAGEMENTLIBRARY_API int deleteFolder(const string& folderPath) {
	//checks if folder exists
	if (!std::filesystem::exists(folderPath))
	{
		cerr << "Error directory path (" << folderPath << ") doesn't exist!\n";
		return 1;
	}
	//if it does delete its contents and remove the folder
	else {
		deleteDirectoryContents(folderPath);
		std::filesystem::remove(folderPath);
	}
	return 0;
}