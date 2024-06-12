#include <winsock2.h>
#include <iostream>
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
using std::mutex;
using std::lock_guard;
using std::promise;
using std::future;
using std::to_string;

//creates typedefs for the file management functions used in this file
typedef int (*funcCreateDirectory)(const string& dirPath);
typedef int (*funcIsDirectoryPresent)(const string& dirPath);
typedef int (*funcIsDirectoryEmpty)(const string& dirPath);
typedef int (*funcDeleteDirectoryContents)(const string& dirPath);
typedef string(*funcReadDatafromFile)(const string& filePath);
typedef int (*funcCreateFile)(const string& filePath);
typedef int (*funcWriteToFile)(const string&, const string&);
typedef int (*funcDeleteFolder)(const string&);
typedef PMapLibrary* (*Map_Factory)();
typedef PReduceLibrary* (*Reduce_Factory)();
typedef PSortLibrary* (*Sort_Factory)();
#pragma comment(lib, "Ws2_32.lib")

#define PORT 8080
#define BUFLEN 512

//function run by map threads that maps the words in the file to the file name
void mapFunction(PMapLibrary* pMap, string dirPath, string fileContent)
{
	pMap->map(dirPath, fileContent);
}

//function run by the reduce threads that reduces the words in the file to the output file
int sortFunction(PSortLibrary* pSort, string tempDir, string outputDir, string fileName)
{
	// Call the create_word_map function, which goes through the temp directory and returns a map
		// with all the words in the temp directory files and a vector of of the numbers associated with the word
	map <string, vector<int>> words = pSort->create_word_map(tempDir);
	// Load the DLLs
	int isSuccessful = 0;
	HINSTANCE reduceDLL;
	const wchar_t* reducelibName = L"ReduceLibrary";
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
		//let the user know if the reduce library is not loaded
		cerr << "Error:Unable to load Reduce Library.";
		isSuccessful = 1;
	}
	//return 0 if successful and 1 if not
	return isSuccessful;
}

int main() {
	WSADATA wsaData;
	SOCKET serverSocket, clientSocket;
	sockaddr_in serverAddr, clientAddr;
	int clientAddrSize = sizeof(clientAddr);
	char buffer[BUFLEN];

	// Initialize Winsock
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
		std::cerr << "WSAStartup failed.\n";
		return 1;
	}

	// Create a socket
	serverSocket = socket(AF_INET, SOCK_STREAM, 0);
	if (serverSocket == INVALID_SOCKET) {
		std::cerr << "Socket creation failed.\n";
		WSACleanup();
		return 1;
	}

	// Prepare the sockaddr_in structure
	serverAddr.sin_family = AF_INET;
	serverAddr.sin_addr.s_addr = INADDR_ANY;
	serverAddr.sin_port = htons(PORT);

	// Bind the socket
	if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
		std::cerr << "Bind failed.\n";
		closesocket(serverSocket);
		WSACleanup();
		return 1;
	}

	// Listen for incoming connections
	if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR) {
		std::cerr << "Listen failed.\n";
		closesocket(serverSocket);
		WSACleanup();
		return 1;
	}

	std::cout << "Waiting for connections...\n";

	// Accept a client socket
	clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientAddrSize);
	if (clientSocket == INVALID_SOCKET) {
		std::cerr << "Accept failed.\n";
		closesocket(serverSocket);
		WSACleanup();
		return 1;
	}

	std::cout << "Client connected.\n";

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
	// For this program, the temp directory name is "temps"
	string tempDir;

	// Receive data from the client
	int bytesReceived1 = recv(clientSocket, buffer, BUFLEN, 0);
	if (bytesReceived1 > 0)
	{
		buffer[bytesReceived1] = '\0'; // Null-terminate the received data
		inputDir = "..\\" + (string)buffer;
		std::cout << "Message from client bytes received: " << buffer << std::endl;
	}

	int bytesReceived2 = recv(clientSocket, buffer, BUFLEN, 0);
	if (bytesReceived2 > 0)
	{
		buffer[bytesReceived2] = '\0'; // Null-terminate the received data
		outputDir = "..\\" + (string)buffer;
		std::cout << "Message from client bytes received: " << buffer << std::endl;
	}

	int bytesReceived3 = recv(clientSocket, buffer, BUFLEN, 0);
	if (bytesReceived3 > 0)
	{
		buffer[bytesReceived3] = '\0'; // Null-terminate the received data
		tempDir = "..\\" + (string)buffer;
		std::cout << "Message from client bytes received: " << buffer << std::endl;
	}


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
		funcDeleteFolder deleteFolder = (funcDeleteFolder)GetProcAddress(fileDLL, "deleteFolder");

		std::cout << "Message from client directory loading: " << buffer << std::endl;

		//if the file management functions are found proceed with program
		if (isDirectoryPresent != NULL && isDirectoryEmpty != NULL && deleteDirectoryContents != NULL && readDatafromFile != NULL && createFile != NULL) {

			std::cout << "Message from client directory present: " << buffer << std::endl;

			string bufferInput3;
			while (bufferInput3 != "3")
			{
				int bytesReceived = recv(clientSocket, buffer, BUFLEN, 0);
				bufferInput3 = buffer;
				if (bytesReceived > 0) {

					buffer[bytesReceived] = '\0'; // Null-terminate the received data

					std::cout << "Message from client response: " << buffer << std::endl;

					string bufferInput = buffer;
					if (bufferInput == "1")
					{
						std::cout << "Message from client: " << buffer << std::endl;
						// Create a vector of threads to run the map function
						vector<thread> mapThreads;

						// Iterate through the input files in the input directory
						for (const auto& entry : std::filesystem::directory_iterator(inputDir))
						{
							// Create a map object for each file so that each thread has its
							// own unique pMap
							auto pMap = mapFactory();
							// Set the temp directory in the pMap object
							pMap->setTempDirectory(tempDir);
							// Read each file and output its contents
							string fileContent = readDatafromFile(entry.path().string());
							// Create a thread to run the map function
							// which is pMap->map(dirPath, fileContent);
							mapThreads.emplace_back(mapFunction, pMap, entry.path().filename().string(), fileContent);
						}

						// Wait for all the threads to finish
						for (auto& thr : mapThreads)
						{
							thr.join();
						}
						// Send response to the client
						const char* response = "Complete";
						send(clientSocket, response, strlen(response), 0);
					}

					std::cout << "Message from client end of buffer1: " << buffer << std::endl;

					string bufferInput2 = buffer;
					if (bufferInput2 == "2")
					{
						std::cout << "Message from client2: " << buffer << std::endl;

						//variable to hold the number of folders the temp files will be divided into
						int numOfFolders = 4;
						//varible to hold the names of the folders
						vector<string> reduceDir;

						cout << "this is the tempDir1" << tempDir << "\n";
						// Create the reduce directories
						for (int i = 0; i < numOfFolders; i++) {
							string reduceDirName = tempDir + "\\reduceDir" + to_string(i);
							cout << "This is the reducedirName " << reduceDirName << "\n";
							reduceDir.push_back(reduceDirName);
							if (createDirectory(reduceDirName) != 0) {
								cerr << "Error: Failed to create directory " << reduceDirName << "\n";
								return 1;
							}
						}
						cout << "this is the tempDir2" << tempDir << "\n";

						//variable used as a counter to determine which folder the file will be placed in
						int fileNumber = 1;
						for (const auto& entry : std::filesystem::directory_iterator(tempDir))
						{
							// Skip the file if it is a directory
							//variable to hold the name of the current file
							string fileName = entry.path().filename().string();
							//variable to determine if the file name matches any of the folder names
							bool isFolderName = false;
							//cycle through the folder names to check if the file name matches
							for (const auto& folderName : reduceDir)
							{
								if (fileName == std::filesystem::path(folderName).filename().string())
								{
									isFolderName = true;
									break;
								}
							}
							if (isFolderName)
							{
								continue; // Skip this file if its name matches any folder name in reduceDir
							}

							// Determine which folder the file will be placed in
							int folderNumber = fileNumber % numOfFolders;
							// Create the file in the appropriate reduce directory
							string reduceFileName = reduceDir[folderNumber] + "\\" + entry.path().filename().string();
							if (createFile(reduceFileName) != 0) {
								cerr << "Error: Failed to create file " << reduceFileName << "\n";
								return 1;
							}
							// Read the file and write its contents to the file in the reduce folder
							string fileContent = readDatafromFile(entry.path().string());
							if (writeDataToFile(reduceFileName, fileContent) != 0) {
								cerr << "Error: Failed to write data to file " << reduceFileName << "\n";
								return 1;
							}
							fileNumber++;
						}

						//create a directory to hold the intermediate output files
						string tempOutputDir = tempDir + "\\tempOutput";
						reduceDir.push_back(tempOutputDir);
						if (createDirectory(tempOutputDir) != 0)
						{
							cerr << "Error: Failed to create directory " << tempOutputDir << "\n";
							return 1;
						}

						//vector of threads to run the sort and reduce function
						vector<thread> reduceThreads;
						//vector to hold the results of the threads
						vector<int> results;
						//lock to ensure that the results vector is not accessed by multiple threads at the same time
						mutex mtx;
						for (int i = 0; i < numOfFolders; i++)
						{
							// Create a sort object
							auto pSort = sortFactory();
							//determine the output file name
							string fileName = "output" + to_string(i) + ".txt";
							// Create a thread to run the sort and reduce functions the function will return 0 if successful and 1 if not, the result is added to the results vector
							reduceThreads.emplace_back([&results, &mtx, pSort, reduceDir, tempOutputDir, fileName, i]()
								{
									int result = sortFunction(pSort, reduceDir[i], tempOutputDir, fileName);
									{
										lock_guard<mutex> lock(mtx);
										results.push_back(result);
									}
								});
						}

						//variable to hold the sum of the results vector
						int isSuccessful = 0;
						//cycle through the results vector and add the results to the isSuccessful variable
						for (const auto& result : results)
						{
							isSuccessful += result;
						}
						// Wait for all the threads to finish
						for (auto& thr : reduceThreads)
						{
							thr.join();
						}

						// Create a variable to hold the output file name
						string fileName = "output.txt";
						//calls function that runs sort and reduce on the temp output directory
						isSuccessful += sortFunction(sortFactory(), tempOutputDir, outputDir, fileName);

						//Delete the temp reduce directories
						for (auto folder : reduceDir)
						{
							isSuccessful += deleteFolder(folder);
						}

						// If the previous loop was able to add all of the key, sum pairs to the file
						// an success file is created in the output directory
						if (isSuccessful == 0)
						{
							int createOutput = createFile(outputDir + "\\Success.txt");
						}

						// Program is complete
						cout << "\nProgram complete.\n";
						// Send response to the client
						const char* response = "Complete2";
						send(clientSocket, response, strlen(response), 0);
					}
				}
				//inform user if the file management functions are not found
			}

			//free the libraries loabed
			FreeLibrary(fileDLL);
			FreeLibrary(mapDLL);
			FreeLibrary(reduceDLL);
			FreeLibrary(sortDLL);
		}
		//if the DLLs don't load inform user
		else
		{
			cerr << "Error: Failed to load one or more DLLs.\n";
		}
	}
	// Cleanup
	closesocket(clientSocket);
	closesocket(serverSocket);
	WSACleanup();

	return 0;
}