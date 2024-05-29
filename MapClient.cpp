/*
Chandler Nunokawa
Dana Marie Castillo Chea
James Lu
Sarah Welvaert

Professor Scott Roueche
CSE-687 Object Oriented Design

Syracuse University
Project Phase 3
05/29/2024
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
#include <future>
#define DllImport   __declspec( dllimport )


using std::string;
using std::map;
using std::vector;
using std::cout;
using std::cin;
using std::cerr;
using std::thread;
using std::promise;
using std::future;

//creates typedefs for the file management functions used in this file
typedef int (*funcCreateDirectory)(const string& dirPath);
typedef int (*funcIsDirectoryPresent)(const string& dirPath);
typedef int (*funcIsDirectoryEmpty)(const string& dirPath);
typedef int (*funcDeleteDirectoryContents)(const string& dirPath);
typedef string(*funcReadDatafromFile)(const string& filePath);
typedef int (*funcCreateFile)(const string& filePath);
typedef int (*funcWriteToFile)(const string&, const string&);
typedef PMap* (*Map_Factory)();
typedef PReduceLibrary* (*Reduce_Factory)();
typedef PSort* (*Sort_Factory)();

void f1(PMap* pMap, string dirPath, string fileContent)
{
	pMap->map(dirPath, fileContent);
}

int f2(PSort* pSort, string tempDir, string outputDir, string fileName)
{
	// Call the create_word_map function, which goes through the temp directory and returns a map
		// with all the words in the temp directory files and a vector of of the numbers associated with the word

	map <string, vector<int>> words = pSort->create_word_map(tempDir);
	// Load the DLLs
	int isSuccessful = 0;
	HINSTANCE reduceDLL;
	const wchar_t* reducelibName = L"Reduce";
	reduceDLL = LoadLibraryEx(reducelibName, NULL, NULL);
	if (reduceDLL != NULL) {
		//loads the reduce class factory function from the DLL
		auto reduceFactory = reinterpret_cast<Reduce_Factory>(GetProcAddress(reduceDLL, "createReduce"));
		if (!reduceFactory) {
			cerr << "Error: Unable to find reduce factory\n";
			return 1;
		}

		// Loop to run through the string vector pairs in the map and use a reduce class
		// to add the word and the vector sum to an output file in the output directory
		for (const auto& pair : words)
		{

			// Creates a Reduce class that saves the output directory
			auto pReduce = reduceFactory();
			//if the reduce class is not created, inform the user
			if (!pReduce) {
				cerr << "Error: reduce factory failed\n";
				return 1;
			}
			pReduce->setOutputDirectory(outputDir, fileName);
			//// Call the reduce function that adds together the vector to create a vector sum
			//// and outputs the key and the sum to a file in the output directory
			//// Reduce function returns 0 if it is successful at adding it and returns 1 if unsuccessful,
			//// that number is added to isSuccessful
			//isSuccessful = isSuccessful + pReduce->reduce(pair.first, pair.second);
			isSuccessful = isSuccessful + pReduce->reduce(pair.first, pair.second);
		}
	}
	else {
		cerr << "Error:Unable to load Reduce Library.";
		isSuccessful = 1;
	}
	return isSuccessful;
}

int main(int argc, char* argv[])
{
	// Load the DLLs
	HINSTANCE fileDLL;
	HINSTANCE mapDLL;
	HINSTANCE reduceDLL;
	HINSTANCE sortDLL;
	const wchar_t* filelibName = L"FileManagement";
	const wchar_t* maplibName = L"MapLibrary";
	const wchar_t* reducelibName = L"Reduce";
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
	string tempDir2 = "temps";

	if (fileDLL != NULL && mapDLL != NULL && reduceDLL != NULL && sortDLL != NULL) {
		//loads the map class factory function from the DLL
		auto mapFactory = reinterpret_cast<Map_Factory>(GetProcAddress(mapDLL, "createMap"));
		if (!mapFactory) {
			cerr << "Error: Unable to find map factory\n";
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
		funcCreateDirectory createDirectory = (funcCreateDirectory)GetProcAddress(fileDLL, "createDirectory");
		funcWriteToFile writeDataToFile = (funcWriteToFile)GetProcAddress(fileDLL, "writeDataToFile");


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
				deleteDirectoryContents(tempDir);

				// Ensure that the output directory is empty
				deleteDirectoryContents(outputDir);
			}

			vector<thread> mapThreads;
			// Iterate through the input files in the input directory
			for (const auto& entry : std::filesystem::directory_iterator(inputDir))
			{
				auto pMap = mapFactory();
				pMap->setTempDirectory(tempDir1);
				// Read each file and output its contents
				string fileContent = readDatafromFile(entry.path().string());
				mapThreads.emplace_back(f1, pMap, entry.path().filename().string(), fileContent);
			}

			for (auto& thr : mapThreads)
			{
				thr.join();
			}

			int numOfFolders = 2;
			vector<string> reduceDir;
			for (int i = 0; i < numOfFolders; i++) {
				string reduceDirName = tempDir + "\\reduceDir" + std::to_string(i);
				reduceDir.push_back(reduceDirName);
				if (createDirectory(reduceDirName) != 0) {
					cerr << "Error: Failed to create directory " << reduceDirName << "\n";
					return 1;
				}
			}

			int fileNumber = 1;
			for (const auto& entry : std::filesystem::directory_iterator(tempDir)) {

				string fileName = entry.path().filename().string();
				bool isFolderName = false;

				for (const auto& folderName : reduceDir) {
					if (fileName == std::filesystem::path(folderName).filename().string()) {
						isFolderName = true;
						break;
					}
				}

				if (isFolderName) {
					continue; // Skip this file if its name matches any folder name in reduceDir
				}

				int folderNumber = fileNumber % numOfFolders;
				string reduceFileName = reduceDir[folderNumber] + "\\" + entry.path().filename().string();
				if (createFile(reduceFileName) != 0) {
					cerr << "Error: Failed to create file " << reduceFileName << "\n";
					return 1;
				}
				string fileContent = readDatafromFile(entry.path().string());
				if (writeDataToFile(reduceFileName, fileContent) != 0) {
					cerr << "Error: Failed to write data to file " << reduceFileName << "\n";
					return 1;
				}
				fileNumber++;
			}


			string tempOutputDir = tempDir + "\\tempOutput" ;
			reduceDir.push_back(tempOutputDir);
			if (createDirectory(tempOutputDir) != 0) {
				cerr << "Error: Failed to create directory " << tempOutputDir << "\n";
				return 1;
			}

			vector<thread> reduceThreads;
			// Create a varible to determine if all of the words have been added to the file correctly,
			// if they have it will remain 0

			std::vector<int> results;
			std::mutex mtx;
			for (int i = 0; i < numOfFolders; i++)
			{
				auto pSort = sortFactory();
				string fileName = "output" + std::to_string(i) + ".txt";

				reduceThreads.emplace_back([&results, &mtx, pSort, reduceDir, tempOutputDir, fileName, i]() {
					int result = f2(pSort, reduceDir[i], tempOutputDir, fileName);
					{
						std::lock_guard<std::mutex> lock(mtx);
						results.push_back(result);
					}
					});

			}

			// Wait for all the threads to finish
			for (auto& thr : reduceThreads)
			{
				thr.join();

			}
			int isSuccessful = 0;
			for (const auto& result : results) {
				isSuccessful += result;
			}

			// Create a variable to hold the output file name
			string fileName = "output.txt";
			//calls function that runs sort and reduce on the temp output directory
			isSuccessful += f2(sortFactory(), tempOutputDir,outputDir1, fileName);
		
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
