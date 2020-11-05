Architecture of mflix directory

The data directory contains all of the source data we'll be using for the mflix application.
The dotini file that is  going to contain information so that the application can connect to dbaas, as well as other information that the application will use internally.

More on that later.

There are two directories under the mflix directory-- api and build.

	Api contains some flask root handlers for the application, and build contains the front end application.

The db.py file
	It contains all of the methods that interact with the database.

	Initially, most of these would just be stubs that performes required functionality.

	You do not need to have MongoDB installed, as we had used DBAAS.


Factory.py contains functionality that assembles the FOS application for running.

	You don't have to modify this file.

  
	The API layer that handles requests when the application is running is implemented in movies.py and user.py.

	Db.py is where all of the methods that interact with database are located and where you will be doing all of your implementation.

