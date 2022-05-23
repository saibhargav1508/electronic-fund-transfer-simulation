// Sai Bhargav Mandavilli
// University of Florida

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>


// defining structure for transferring amount between accounts
struct transfer_frame{
	int account_from;
	int account_to;
	int amount;
};

// declare function for transferring between accounts
void *transfer_function(void *t_func);

// defining structure for each unique account
// account will have unique id, number and balance
struct account{
	int id;
	int number;
	int balance;
};

// storing all accounts in a tuple data structure
struct account acc_tup[10000];

// declaring global variables
char data[100000], temp[100000];
struct transfer_frame t[10000];
FILE *fp;
int acc_n = 0, t_count = 0, workers_num;
    	
// defining mutex variables
static pthread_mutex_t *mtx;
pthread_t  *lock ;

// function to store account details
void update_accounts();

// function to store transaction details
void load_transactions();

// defining function to handle errors
void errExit(char *errorStr){
	printf("%s", errorStr);
	exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{
    // check if required command line arguments are given
	if(argc <=2) 
		errExit("Please enter both inputFile and number of worker threads\n ");

	// read data from input file
	if((fp = fopen(argv[1], "r")) == NULL)
		errExit("could not find input file\n");
	
	workers_num = atoi(argv[2]);
	
	while(!feof(fp))
	{
		fgets(data, 20000, fp);
		// store account details before beginning transactions
		update_accounts();

		// store transaction details in data structure
		// account to, account from and how much to transfer
		load_transactions();
	}

	// close file pointer
	fclose(fp);
        
	// we use the modified version of dining philosophers scenario
	// to achieve synchronization
	
	// dynamically assign memory to mutex variables
	lock = malloc(workers_num * sizeof(pthread_t));
	
	// since there are acc_n number of accounts, we shall
	// allocate acc_n+1 times memory
	mtx = malloc((acc_n + 1) * sizeof(pthread_mutex_t)); 
        
	int times_tr = 0, s;
	// dynamically initialize mutexes for accounts
	for(int m=1; m<=acc_n; m++)
	{
 		s = pthread_mutex_init(&mtx[m], NULL);
 		if(s != 0)
 			errExit("pthread_mutex_init");
	}
	
	// regardless of the number of worker threads specified this check will ensure the program behaves the same
	if (workers_num > t_count)
        workers_num = t_count;
    
	while(times_tr < t_count)
	{
		for(int i=0; i< workers_num; i++)
        {
			// creating lock threads               
			s = pthread_create(&lock[i], NULL, transfer_function, &t[times_tr]);
			if(s != 0)
 				errExit("pthread_create");
        	times_tr++;
		}
		
		// wait for locks to terminate
		for (int j=0; j<workers_num; j++)
		{
			s = pthread_join(lock[j], NULL);
			if(s != 0)
 				errExit("pthread_join");
		}
	}
    
	// destroy the lock threads
	for(int m=1; m<=acc_n; m++)
	{
		s = pthread_mutex_destroy(&mtx[m]);
		if(s != 0)
			errExit("pthread_mutex_destroy");
	}
    	
	// print the account and balance after making all transactions
	for(int n=1; n<=acc_n; n++)
	{
		if(acc_tup[n].number != 0)
			printf("%d\t%d \n", acc_tup[n].number, acc_tup[n].balance);
	}
	
	pthread_exit(NULL);
}

void *transfer_function(void *t_func)
{
	// in this function we are trying to process each request by locking the strcuture related to the transfer
	// this is inspired from the text book where the author says we have to lock the data and not the code
	
	struct transfer_frame t_func1 = *((struct transfer_frame*)(t_func));

	// check to filter incorrect inputs
	if(t_func1.account_from < 1 || t_func1.account_to < 1)
		return NULL;
	
	//lock the mutexes while updating account
	pthread_mutex_lock(&mtx[t_func1.account_to]);
	pthread_mutex_lock(&mtx[t_func1.account_from]);

	// update balances
    acc_tup[t_func1.account_from].balance -= t_func1.amount;
	acc_tup[t_func1.account_to].balance += t_func1.amount;	

	//unlock the mutexes after updation
	pthread_mutex_unlock(&mtx[t_func1.account_to]);
	pthread_mutex_unlock(&mtx[t_func1.account_from]);
}

void update_accounts()
{
	int i;
	// logic to add account details to tuple data structure
	if(data[0] != 'T')
	{
		// increment for each unique account number
		acc_n++;
		strcpy(temp,data); // storing in temp so we don't overwrite data
		
		for (i=0; i<20000; i++)
		{
			// replace space in input string with /0
			if (data[i] == ' ')
			{
				data[i] = '\0';
				acc_tup[acc_n].id= acc_n;
				acc_tup[acc_n].number = atoi(data);
				acc_tup[acc_n].balance = atoi(i+temp+1); // use temp here to avoid data race conditions
				//printf("%d %d %d", acc_tup[acc_n].id, acc_tup[acc_n].number, acc_tup[acc_n].balance);
			}
		}
	}
}

void load_transactions()
{
	int tr_n = 0; // count number of transfers
	int i;
	
	if(data[0] == 'T' && data[1] == 'r')
	{
		strcpy(temp, data+8); // +8 to remove 'transfer' from string
		memset(data, 0, sizeof(data));
		int t_len = strlen(temp);

		for (i=t_len; i>=0; i--)
		{
			// for the first transfer
			// we add amount to the data structure
			if(temp[i] == ' ' && tr_n == 0)
			{   
				t[t_count].amount = atoi(temp + i + 1);
				tr_n++;
            			
				// copy this modified string back to original data
				temp[i] = '\0';
				strncpy(data,temp,i);
				memset(temp, 0, sizeof(temp)); // clearing temp to process next transfer tuple
        	}
                
			// storing account_from details
			else if(temp[i] == ' ' && tr_n == 2)
			{
				int j = atoi(temp + i + 1); // storing id of account number for further transaction
				for(int k=1; k<=acc_n; k++)
				{
					if(acc_tup[k].number == j)
						t[t_count].account_from = acc_tup[k].id;
				}
				tr_n++;
			}
                
			// storing account_to details
			// similar to account_from logic
			if(data[i] == ' ' && tr_n == 1)
       		{
                    
				int j = atoi(data + i + 1);
				for(int k=1; k<=acc_n; k++)
				{
					if(acc_tup[k].number == j)
						t[t_count].account_to = acc_tup[k].id;
				}
				tr_n++;
				data[i] = '\0'; // to process next string
				strncpy(temp, data, i);
				memset(data, 0, sizeof(data));
			}
		}
		t_count++;   // to point to next transfer string
	}
	// clear both data and temp for next iteration
	memset(temp, 0, sizeof(temp));
	memset(data, 0, sizeof(data));	
}
