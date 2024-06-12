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
#include <iostream>
#include <filesystem>
#include <thread>
#include <future>
#include <winsock2.h>
#include <WS2tcpip.h>
#define DllImport   __declspec( dllimport )

#pragma comment(lib, "Ws2_32.lib")

#define PORT 8080
#define SERVER_IP "127.0.0.1"
#define BUFLEN 512

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

int main(int argc, char* argv[])
{
	WSADATA wsaData;
	SOCKET clientSocket;
	sockaddr_in serverAddr;

	// Initialize Winsock
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
		std::cerr << "WSAStartup failed.\n";
		return 1;
	}

	// Create a socket
	clientSocket = socket(AF_INET, SOCK_STREAM, 0);
	if (clientSocket == INVALID_SOCKET) {
		std::cerr << "Socket creation failed.\n";
		WSACleanup();
		return 1;
	}

	// Prepare the sockaddr_in structure
	serverAddr.sin_family = AF_INET;
	serverAddr.sin_port = htons(PORT);
	inet_pton(AF_INET, SERVER_IP, &serverAddr.sin_addr);

	// Connect to the server
	if (connect(clientSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
		std::cerr << "Connect failed.\n";
		closesocket(clientSocket);
		WSACleanup();
		return 1;
	}

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
	char inputDir[BUFLEN];
	// For this program, the output directory name is "outputs"
	char outputDir[BUFLEN];
	// For this program, the temp directory name is "temps"
	char tempDir[BUFLEN];

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

			send(clientSocket, inputDir, strlen(inputDir), 0);

			// Prompt user for output directory
			int outputDirectory = 1;
			while (outputDirectory)
			{
				cout << "Please type in a valid output directory.\n";
				cin >> outputDir;
				outputDirectory = isDirectoryPresent(outputDir);
			}

			send(clientSocket, outputDir, strlen(outputDir), 0);

			// Prompt user for temporary directory
			int tempDirectory = 1;
			while (tempDirectory)
			{
				cout << "Please type in a valid temp directory.\n";
				cin >> tempDir;
				tempDirectory = isDirectoryPresent(tempDir);
			}

			send(clientSocket, tempDir, strlen(tempDir), 0);

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

			// Send data to the server

			char buffer[BUFLEN] = "1";
			send(clientSocket, buffer, strlen(buffer), 0);

			// Receive response from the server
			int bytesReceived = recv(clientSocket, buffer, BUFLEN, 0);
			if (bytesReceived > 0) {
				buffer[bytesReceived] = '\0'; // Null-terminate the received data
				std::cout << "Response from server: " << buffer << std::endl;
			}

			string bufferInput = buffer;
			if (bufferInput == "Complete")
			{
				char buffer2[BUFLEN] = "2";
				// Send data to the server
				send(clientSocket, buffer2, strlen(buffer2), 0);

				// Receive response from the server
				int bytesReceived = recv(clientSocket, buffer, BUFLEN, 0);
				if (bytesReceived > 0) {
					buffer[bytesReceived] = '\0'; // Null-terminate the received data
					std::cout << "Response from server: " << buffer << std::endl;
				}
			}

			string bufferInput2 = buffer;

			if (bufferInput2 == "Complete2")
			{
				char buffer3[BUFLEN] = "3";
				// Send data to the server
				send(clientSocket, buffer3, strlen(buffer3), 0);
			}
		}
		//inform user if the file management functions are not found
		else
		{
			cerr << "Error: Function not found in one of the DLLs.\n";
		}

		//free the libraries loabed
		FreeLibrary(fileDLL);
		FreeLibrary(mapDLL);
		FreeLibrary(reduceDLL);
		FreeLibrary(sortDLL);

		closesocket(clientSocket);
		WSACleanup();
	}
	//if the DLLs don't load inform user
	else
	{
		cerr << "Error: Failed to load one or more DLLs.\n";
	}
}
