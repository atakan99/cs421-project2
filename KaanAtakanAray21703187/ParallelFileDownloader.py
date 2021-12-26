''' if you are using Python, it should run with: 
python3 ParallelFileDownloader.py <index_file> <connection_count> 
command, where <index file> is the URL of the index that includes a list of text file 
URLs to be downloaded and <connection count> is the number of connections to be 
established for each file URL. 

•  Please refer to W3Cs RFC 2616 for details of the HTTP messages. 
•  You will assume that each line of the index file includes one file URL. 
•  You will assume that the name of each file in the index is unique. 
•  Your program will not save the index file to the local folder. 
•  The number of bytes downloaded through each connection should differ by at most one 
byte. The number of bytes downloaded through each connection is n/k if n is divisible by 
k,  where  n and  k  respectively  denote  the  number  of  bytes  in  the  file and  the  number  of 
connections.  Otherwise,  (⌊n/k⌋+1) bytes should be downloaded through the first 
(n-⌊n/k⌋*k) connections and ⌊n/k⌋  bytes  should  be  downloaded  through  the  remaining 
connections. 
•  Your program should print a message to the command-line to inform the user about the 
status of the files. 
3 
•  The  downloaded  file  should  be  saved  under  the  directory  containing  the  source  file 
ParallelFileDownloader.java  or  ParallelFileDownloader.py  and  the name  of  the  saved  file 
should be the same as the name of the downloaded file. 
 
• You may use the following URLs to test your program:
    www.cs.bilkent.edu.tr/~cs421/fall21/project1/index1.txt
    www.cs.bilkent.edu.tr/~cs421/fall21/project1/index2.txt
• Please contact your assistant if you have any doubt about the assignment.
'''

'''
HTTP/1.1 404 Not Found
HTTP/1.1 200 OK
'''

'''
{'date': 'Wed, 10 Nov 2021 12:12:08 GMT', 'server': 'Apache/2.4.25 (FreeBSD) OpenSSL/1.0.2u-freebsd PHP/7.4.15', 
'last-modified': 'Mon, 25 Oct 2021 17:48:47 GMT', 'etag': '"b-5cf30f914cb18"', 'accept-ranges': 'bytes', 
'content-length': '11', 'content-type': 'text/plain', 'http': 'HTTP/1.1 200 OK', 'body': 'Cras nunc.\n'}

{'date': 'Wed, 10 Nov 2021 12:12:29 GMT', 'server': 'Apache/2.4.25 (FreeBSD) OpenSSL/1.0.2u-freebsd PHP/7.4.15', 
'content-length': '238', 'content-type': 'text/html; charset=iso-8859-1', 
'http': 'HTTP/1.1 404 Not Found', 
'body': '<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">\n<html><head>\n<title>404 Not Found</title>\n</head><body>\n<h1>Not Found</h1>\n<p>The requested URL /~cs421/fall21/project1/files2/dummy5.txt was not found on this server.</p>\n</body></html>\n'}
'''

import socket
import argparse
import os
import sys
import math
from functools import partial
# threading is concurrent
import threading
from itertools import repeat
import multiprocessing
#concurrent.futures is a better way 
import concurrent.futures


class ParallelFileDownloader:

    @classmethod
    def separate_website_and_file_names(cls,url):
        ''' 
        takes the url and seperates the filename and host addr
        return [host_addr, file_name]
        '''
        website_url = url.split('/')[0]
        file_name = ''.join(url.partition('/')[1:])
        return[website_url, file_name]

    @classmethod
    def formatted_http_get(cls,file_name,host_addr):
        return "GET /{0} HTTP/1.1\r\nHost:{1}\r\n\r\n".format(file_name,host_addr)

    @classmethod
    def formatted_http_partial_get(cls,file_name ,host_addr ,range):
        return   "GET {0} HTTP/1.1\r\nhost:{1}\r\nrange: bytes={2}\r\n\r\n".format(file_name,host_addr,range) 

    @classmethod
    def formatted_http_head(cls,file_name,host_addr):
        return "HEAD /{0} HTTP/1.1\r\nHost:{1}\r\n\r\n".format(file_name,host_addr)

    @classmethod
    def create_socket(cls):
        try:
            my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error:
            print('Failed to create socket')
            sys.exit()
        return my_socket

    @classmethod
    def connect_to_host(cls,host_addr,my_socket, port):
        try:
            index_file_ip = socket.gethostbyname( host_addr )
        except socket.gaierror:
            print('index_file_host ip could not be recieved, exiting')
            sys.exit()

        my_socket.connect(( index_file_ip, port))

    @classmethod    
    def close_socket(cls,my_socket):
        my_socket.close

    @classmethod
    def get_index_file_list(cls,string):
        index_file_addresses = string.split('\n')
        index_file_addresses.pop()
        return index_file_addresses

    @classmethod
    def dictify_response(cls,response):
       
        header, _, body = response.partition('\r\n\r\n')
        header = header.split('\r\n')
        header.append(body)
        json_data = {}
        new_header = header [1:len(header)-1]
        for b in new_header:
            i = b.split(': ')
            json_data[i[0].lower()] = i[1]
        json_data['http'] = header[0]
        json_data['body'] = header[len(header)-1]
        return json_data

    @classmethod
    def send_http_req(cls,  http_req, host_addr,port):
        '''sends http request and returns the response as a dictionary '''
        my_socket = cls.create_socket() 
        cls.connect_to_host(host_addr, my_socket,port)
        response = ''
        my_socket.sendall(http_req.encode('utf-8'))
        while True:
            recv = my_socket.recv(2048)
            if recv == b'':
                break
            response += recv.decode()
        cls.close_socket(my_socket)
        dict_res = cls.dictify_response(response)
        return dict_res

    @classmethod 
    def abc(cls, req, a):
        return '{}-{}'.format(req,a)
    @classmethod
    def download_index_files_parallel(cls, file_list, port, connection_count):
        ''' downloads index files in parallel, '''
        for index, value in enumerate(file_list, 1):
            # initializations
            temp = cls.separate_website_and_file_names(value)
            host_addr = temp[0]
            file_name = temp[1]
            req = cls.formatted_http_head(file_name,host_addr)
            dict_res = cls.send_http_req(req, host_addr, port)
            # check errors
            if dict_res['http'] != 'HTTP/1.1 200 OK':
                print('{}. {} {}'.format(index, value, 'is not found'))
                continue
            a =  file_name.split('/')
          
            #get correct file name to save 
            fname = a[len(a)-1]
        
            content_length = dict_res['content-length']

            # algorithm given in the project report 
            n_over_k = int(int(content_length)/int(connection_count))
            i_content_length = int(content_length)
            i_connection_count = int(connection_count)
            increase_in_bounds_if_divides = math.floor(n_over_k)
            increase_in_bounds_if_not_divides = [(math.floor(n_over_k))+1, math.floor(n_over_k)]
           
            # estanblishes different values due to divisibility
            if i_content_length % i_connection_count == 0:
                lower_bound = 0
                upper_bound = n_over_k - 1 
                first_connections = -1
            else:
                lower_bound = 0
                upper_bound = ((math.floor(n_over_k))+1)-1
                first_connections = (i_content_length-((math.floor(n_over_k))*i_connection_count))
            
            # 
            counter = 1
            # array that contains partial get requests for each target file
            # changes with each iteration of outer for loop
            requests = []

            # for printing purposes
            file_parts_print = 'File Parts: '

            # loop for creating http parital get  requests and putting them to requests array
            for _ in range(i_connection_count):
                # give lowerbound and upperbound appropriate values which  are calculated above in the algo
                if counter > 1:
                    if first_connections == -1:
                        lower_bound = lower_bound + increase_in_bounds_if_divides
                        upper_bound = upper_bound + increase_in_bounds_if_divides
                    else:
                        if counter <= first_connections:
                            lower_bound = lower_bound + increase_in_bounds_if_not_divides[0]
                            upper_bound = upper_bound+increase_in_bounds_if_not_divides[0]
                        else:
                            if counter - first_connections == 1:
                                lower_bound = lower_bound + increase_in_bounds_if_not_divides[1]+1
                                upper_bound = upper_bound + increase_in_bounds_if_not_divides[1]
                            else:
                                lower_bound = lower_bound + increase_in_bounds_if_not_divides[1]
                                upper_bound = upper_bound + increase_in_bounds_if_not_divides[1]
                range_value = '{}-{}'.format(int(lower_bound),int(upper_bound))
                
                #  get partial get request string with appropriate range
                req = cls.formatted_http_partial_get(file_name,host_addr,range_value)
                counter = counter + 1
                requests.append(req)
                file_parts_print += ' {}({}) '.format(range_value, upper_bound-lower_bound+1)

            # this is they way to create parallel connections
            # ProcessPoolExecutor() used rather than ThreadPoolExecutor() to achieve parallelity
            # Because of GIL threads are not parallel in python
            with concurrent.futures.ProcessPoolExecutor() as executor:
                make_request = partial(cls.send_http_req,  port = port, host_addr= host_addr)
                # map used to get responses in order that their request have sent
                results =  executor.map(make_request,requests)
            for result in results: 
                with open("{}".format(os.getcwd() + '/{}'.format(fname)), "a") as text_file:
                    text_file.write(result['body'])
            print('{}.{} (size = {}) {}'.format(index, value,i_content_length, 'is downloaded'))
            print(file_parts_print)
               





################################################################################

if __name__ == '__main__':
    PORT = 80
 
    parser = argparse.ArgumentParser(description='downloads files within requested size parametes')

    parser.add_argument('index_file',  nargs=1 ,metavar='index_file', type=str, help='enter the address of the index file')
    parser.add_argument('connection_count', metavar='connection_count', type=str, help='enter the connection number')
    args = parser.parse_args()
    index_file = args.index_file[0]
    connection_count = args.connection_count


    host_and_fname = ParallelFileDownloader.separate_website_and_file_names(index_file)
    
    request = ParallelFileDownloader.formatted_http_get(host_and_fname[1], host_and_fname[0])
    json_res = ParallelFileDownloader.send_http_req(request,host_and_fname[0], PORT)

    if json_res['http'] != 'HTTP/1.1 200 OK':
        print('{}. {} {}'.format('index is not found please enter a new index'))
        sys.exit();
    addr_list = ParallelFileDownloader.get_index_file_list(json_res['body'])

    print('There are {} files in the index '.format(len(addr_list)))

    ParallelFileDownloader.download_index_files_parallel(addr_list, PORT, connection_count)




