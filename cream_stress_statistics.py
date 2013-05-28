#!/usr/bin/env python

# Copyright 2013 Dimosthenes Fioretos dfiore -at- noc -dot- uoa -dot- gr
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


#########################################################################################################
#                                       NOTES                                                           #
#-------------------------------------------------------------------------------------------------------#
# Version       |   1.1                                                                                 #
#-------------------------------------------------------------------------------------------------------#
# Dependencies  |   pexpect                                                                             #
#               |   python-matplotlib                                                                   #
#-------------------------------------------------------------------------------------------------------#
# Invocation    |   Run script with -h argument                                                         #
#-------------------------------------------------------------------------------------------------------#
#    Notes      |                                                                                       #
#########################################################################################################


from multiprocessing import Process, Lock, Queue, queues
import multiprocessing
import pexpect, re, sys, time, subprocess , shlex, os, signal, math, datetime, ConfigParser, itertools, math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

class _error(Exception):
    def __init__(self,string):
        self.string = string
    def __str__(self):
        return str(self.string)
###############################################################################################################
class sync_queue(multiprocessing.queues.Queue):
        '''
                A multiprocessing.queues.Queue object with a lock implemented to sync its operations.
                Supports the add() and remove() operations.
        '''

        def __init__(self):
                self.q = Queue()
                self.lock = Lock()

        def add(self, item):
                #with self.lock:             #"with" supports lock objects automagically
                        #self.q.put(item)    # dropped it for traditional acquire()/release(), for readability mainly.
                self.lock.acquire()
                self.q.put(item)
                self.lock.release()

        def remove(self):
                return self.q.get()
###############################################################################################################
def create_script(iostat, vmstat, sar, qstat, ps, delay, output_dir):
        '''
                Create a script used to monitor a host.
                The tools used are iostat, vmstat, sar, ps and qstat (each optional).
                The value of each parameter should be 'True' or 'False', with the exception of:
                ps         : a comma separated string (of strings)
                delay      : an int (delay between monitor operations)
                output_dir : the directory to store the script
        '''

        name = 'cream_stress_test_monitor_script_' + str(time.time()) + '_' + str(os.getpid()) + '.sh'
        path = output_dir + '/' + name
        script_file = open(path,'w')

        cnts =  '#!/bin/bash\n\n'+\
                '#at_exit()\n'+\
                '#{\n'+\
                '\t#echo "Killing processes and exiting..."\n'+\
                '\t#killall iostat\n'+\
                '\t#killall vmstat\n'+\
                '\t#killall sar\n'+\
                '\t#exit 0\n'+\
                '#}\n\n'+\
                '\n#trap at_exit SIGINT\n\n'

        if iostat == 'True' or iostat == True:
                cnts =  cnts + 'iostat -k -d '+str(delay)+'  > iostat.dat &\n'
        if vmstat == 'True' or vmstat == True:
                cnts =  cnts + 'vmstat -n '+str(delay)+'  > vmstat.dat &\n'
        if sar == 'True' or sar == True:
                #LC_TIME to show timestamps in 24 hour format
                #(will not output PM/AM, good for parsing both RHEL and DEBIAN hosts)
                cnts =  cnts + 'LC_TIME=POSIX sar -n DEV '+str(delay)+'  > sar.dat &\n'

        #if len(ps) > 0 or qstat == 'True':
        if ps != False or qstat == 'True':
                cnts =  cnts +\
                        '\nwhile true\n'+\
                        'do\n'
                if ps != False:
                        for proc in ps.split(','):
                                cnts =  cnts +\
                                        '\tps -C '+proc+' --no-headers -o %cpu,size >> '+proc+'.dat\n'
                                cnts = cnts + 'echo "---+++---" >> '+proc+'.dat\n'
                if qstat == 'True':
                        cnts = cnts +\
                                '\tqstat | wc -l >> qstat.dat\n'
                cnts =  cnts +\
                        '\tsleep '+str(delay)+'\n'+\
                        'done\n'

        script_file.write(cnts)
        script_file.close()
        print "Monitoring script saved as: " + path

        return path
###############################################################################################################
def execute_noninteractive_ssh_com(command,host,user,port=22,background=False):
        '''
                 Execute a non interactive command through ssh on another host                                     
                 NOTE: Shell metacharacters are NOT recognized. Call them through 'bash -c'.                       
                 Example: '/bin/bash -c "ls -l | grep LOG > log_list.txt"'                                         
                 Arguments:      host                  : ssh host                                                                   
                                 port                  : ssh port, 22 by default                                                    
                                 user                  : the user name to use for the ssh connection                                
                                 command               : the command to execute                                                     
                 Returns:        the command's output                                                                               
        '''

        port=int(port)  #safeguard

        ssh_com = 'ssh -p ' + str(port) +\
                  ' ' + user + '@' + host + ' "echo \"authenticated:\" ; ' + command + '"'
        if background is True or background == "True":
                ssh_com = ssh_com + ' &'

        expect_key = 'Are you sure you want to continue connecting'
        expect_pass = 'password:'
        expect_eof = pexpect.EOF
        expect_auth = "authenticated:"
        ikey = 0
        ipasswd = 1
        ieof = 2
        iauth = 3

        child = pexpect.spawn(ssh_com, timeout=604800) #wait for 7 days (lol) at most for each command to finish
        index = child.expect([expect_key, expect_pass, expect_eof, expect_auth])
        if index == ikey:
                print 'Added foreign host key fingerprint...'
                child.sendline('yes')
                child.sendeof()
                index = child.expect([expect_key, expect_pass, expect_eof, expect_auth])
                if index == ipasswd:
                        raise _error('Password authentication is NOT supported.')
                elif index == iauth:
                        #print 'Authenticating and executing...'
                        index = child.expect([expect_key, expect_pass, expect_eof, expect_auth])
                        if index == ieof:
                                #print 'Connection terminated normally...'
                                pass
                        else:
                                raise _error('Pexpect couldn\'t find a proper or right match for "' + ssh_com + '" in "' + \
                                              expect_key + '" "' + expect_pass + '" "EOF"')
                elif index == ieof:
                        raise _error('SSH has prematurely ended. The following output might contain usefull information: \
                                     "' + str(child.before) + '", "' + str(child.after) + '"')
                else:
                        raise _error('Pexpect couldn\'t find a proper or right match for "' + ssh_com + '" in "' + \
                                      expect_key + '" "' + expect_pass + '" "EOF"')
        elif index == ipasswd:
                raise _error('Password authentication is NOT supported.')
                #print "Sending password..."
                #child.sendline(password)
        elif index == iauth:
                #print 'Authenticating and executing...'
                index = child.expect([expect_key, expect_pass, expect_eof, expect_auth])
                if index == ieof:
                        #print 'Connection terminated normally...'
                        pass
                else:
                        raise _error('Pexpect couldn\'t find a proper or right match for "' + ssh_com + '" in "' + \
                                      expect_key + '" "' + expect_pass + '" "EOF"')
        elif index == ieof:
                raise _error('SSH has prematurely ended. The following output might contain usefull information: \
                             "' + str(child.before) + '", "' + str(child.after) + '"')
        else:
                raise _error('Pexpect couldn\'t find a proper or right match for "' + ssh_com + '" in "' + \
                              expect_key + '" "' + expect_pass + '" "EOF"')

        retVal = str(child.before)
        return retVal
###############################################################################################################
def _enisc(command,host,user,port=22,background=False):
        # short name wrapper-wannabe. I hate long function names :P
        return execute_noninteractive_ssh_com(command,host,user,port,background)
###############################################################################################################
def sub_execute_noninteractive_com(com,wait=False):
        '''
                Execute command through subprocess.Popen() and optionally return output.
                NOTE: If you don't want to actually wait, the command should also take care of it by itself,
                      e.g. by running in the background
        '''

        args = shlex.split(com.encode('ascii'))

        #print 'Will execute: ' + com

        if wait is True:
                p = subprocess.Popen( args , stderr=subprocess.STDOUT , stdout=subprocess.PIPE )
                fPtr=p.stdout
                retVal=p.wait()
                output=fPtr.readlines()
                return output
        else:
                subprocess.Popen(args , stderr=subprocess.STDOUT , stdout=open('/dev/null') )
                return None
###############################################################################################################
def _senc(com,wait=False):
        # wrapper
        return sub_execute_noninteractive_com(com,wait)
###############################################################################################################
def ps_graph(l,delay,host,fp,output_dir):
        '''
                l is a list with the monitored proc's name @ its 1st line and then a duet of numbers (cpu,size)
                @ each line after that, one each delay seconds.
                host used to name uniquely and meaningfully the plot files
        '''

        samples=0
        for i in l[0].split('\n'):
            if '---+++---' in i:
                samples += 1

        timeList = [ (x*y) for x in range(1,samples+1) for y in [int(delay)]]

        for proc in l:
                name=proc.split(':')[1]

                tmp1 = re.sub(r'\s', ' ', proc.split(':')[2])     # erase whitespace characters
                tmp2 = tmp1.split(' ')                            # split items
                vals = filter(None,tmp2)                          # remove empty items
                aek  = [ list(x[1]) for x in itertools.groupby(vals, lambda x:x=='---+++---') if not x[0] ]

                cpu = []
                size = []
                for item in aek:
                    cpu.append(math.fsum([float(x) for x in item[0::2]]))
                    size.append(math.fsum([float(x) for x in item[1::2]]))

                sizeKB = [round(float(x)/1024.0,2) for x in size] # memory size in KB rounded to 2 decimal points for better visualization

                if fp:
                        fp.write('##:'+host+':'+name+':cpu'+'\n')
                        fp.write(repr(zip(timeList,cpu))+'\n')
                        fp.write('##:'+host+':'+name+':size'+'\n')
                        fp.write(repr(zip(timeList,sizeKB))+'\n')

                if len(cpu) == 0: #ignore processes which aren't actually running, even if requested
                    continue

                #trim list to the least size
                least_len = min(len(cpu),len(size),len(timeList))
                cpu = cpu[:least_len]
                size = size[:least_len]
                timeList = timeList[:least_len]

                # plot it
                do_plot1(timeList,cpu,'Time(secs)','CPU',output_dir+'/'+host+'_'+name)
                do_plot1(timeList,sizeKB,'Time(secs)','Mem.Size(KB)',output_dir+'/'+host+'_'+name) #convert bytes to KB for mem.size
###############################################################################################################
def sar_graph(s,delay,host,fp,output_dir):
    '''
            s is a string with the monitored host's sar -n DEV output
            fp is either a file object (opened) or None
            rest vars are self-explanatory
    '''

    l = filter(None,re.sub(r'[ \t\r\f\v]+',' ',s).split('\n'))      # sar's contents, with single spaces, split at newlines
    l[:] = [i for i in l if len(i) > 2 ]                              # remove single \n items (in place)

    ifaceLines = [i for i, x in enumerate(l) if "IFACE" in x]     #line no. of line containing "IFACE"
    diffs = [j-i for i,j in zip(ifaceLines[:-1],ifaceLines[1:])]  #all the difference between those lines
                                                                  #(must always be the same for output to be sane)
    if len(set(diffs)) != 1:
            print "Error while parsing sar output. Wrong diffs found. Will not plot graphs."
            return
    ifaceNum = diffs[0]-1                                           #number of interfaces present

    ifaceNames = []
    for line in l[2:]: #first line is irrelevant, second is header
        if 'IFACE' in line:
            break
        else:
            ifaceNames.append(line.split(' ')[1])

    samples=0
    for i in l:
            if "IFACE" in i:
                    samples += 1
    timeList = [ (x*y) for x in range(1,samples+1) for y in [int(delay)]]

    for name in ifaceNames:
        rx = []
        tx = []

        counter=0
        for line in l[1:]: #the first line contains irrelevant data that can confuse parsing
            if name in line:
                counter+=1
                tmp = line.split(' ')
                rx.append(float(tmp[5]))
                tx.append(float(tmp[6]))

        if fp:
            fp.write('##:'+host+':'+name+':transmit'+'\n')
            fp.write(repr(zip(timeList,rx))+'\n')
            fp.write('##:'+host+':'+name+':recieve'+'\n')
            fp.write(repr(zip(timeList,tx))+'\n')

        do_plot2(timeList,rx,'Time(secs)','Receive(KBps)',timeList,tx,"Time(secs)",
                 "Transmit(KBps)",output_dir+'/'+host+'_'+name)
###############################################################################################################
def vmstat_graph(s,delay,host,fp,output_dir):
        '''
                s is a string with the monitored host's vmstat output
                fp is either a file object (opened) or None
                rest vars are self-explanatory
        '''

        l = filter(None,re.sub(r'[ \t\r\f\v]+',' ',s).split('\n'))      # vmstat's contents, with single spaces, split at newlines
        l[:] = [i for i in l if len(i) > 2 ]                              # remove single \n items (in place)

        procs_ready = [] ; procs_sleep = [] ; mem_swap    = [] ; sys_csps    = []
        mem_free    = [] ; mem_buff    = [] ; mem_cache   = [] ; sys_ips     = []
        cpu_user    = [] ; cpu_sys     = [] ; cpu_idle    = [] ; cpu_iowait  = []
        for i in l[2:]:
                tmp = i.split(' ')
                procs_ready.append(tmp[1]) ; procs_sleep.append(tmp[2]) ; mem_swap.append(tmp[3]) ; mem_free.append(tmp[4])
                mem_buff.append(tmp[5]) ; mem_cache.append(tmp[6]) ; sys_ips.append(tmp[11]) ; sys_csps.append(tmp[12])
                cpu_user.append(tmp[13]) ; cpu_sys.append(tmp[14]) ; cpu_idle.append(tmp[15]) ; cpu_iowait.append(tmp[16])

        timeList = [ (x*y) for x in range(1,len(procs_ready)+10) for y in [int(delay)]] #+10 just in case


        if fp:
                fp.write('##:'+host+':procs_ready'+'\n') ; fp.write(repr(zip(timeList,procs_ready))+'\n')
                fp.write('##:'+host+':procs_sleep'+'\n') ; fp.write(repr(zip(timeList,procs_sleep))+'\n')
                fp.write('##:'+host+':mem_swap'+'\n')    ; fp.write(repr(zip(timeList,mem_swap))+'\n')
                fp.write('##:'+host+':mem_free'+'\n')    ; fp.write(repr(zip(timeList,mem_free))+'\n')
                fp.write('##:'+host+':mem_buff'+'\n')    ; fp.write(repr(zip(timeList,mem_buff))+'\n')
                fp.write('##:'+host+':mem_cache'+'\n')   ; fp.write(repr(zip(timeList,mem_cache))+'\n')
                fp.write('##:'+host+':sys_ips'+'\n')     ; fp.write(repr(zip(timeList,sys_ips))+'\n')
                fp.write('##:'+host+':sys_csps'+'\n')    ; fp.write(repr(zip(timeList,sys_csps))+'\n')
                fp.write('##:'+host+':cpu_user'+'\n')    ; fp.write(repr(zip(timeList,cpu_user))+'\n')
                fp.write('##:'+host+':cpu_sys'+'\n')     ; fp.write(repr(zip(timeList,cpu_sys))+'\n')
                fp.write('##:'+host+':cpu_idle'+'\n')    ; fp.write(repr(zip(timeList,cpu_idle))+'\n')
                fp.write('##:'+host+':cpu_iowait'+'\n')  ; fp.write(repr(zip(timeList,cpu_iowait))+'\n')

        do_plot2(timeList,procs_ready,'Time(secs)','Ready Procs',timeList,procs_sleep,'Time(secs)',
                 'Sleeping Procs',output_dir+'/'+host+'_processes')
        do_plot4(timeList,mem_swap,'Time(secs)','Swap',timeList,mem_free,'Time(secs)','Free',
                 timeList,mem_buff,'Time(secs)','Buffers',timeList,mem_cache,'Time(secs)','Cache',
                  output_dir+'/'+host+'_mem_stats')
        do_plot2(timeList,sys_ips,'Time(secs)','Interrupts',timeList,sys_csps,'Time(secs)',
                 'Context_Switches (both per second)',output_dir+'/'+host+'_system_stats')
        do_plot4(timeList,cpu_user,'Time(secs)','User',timeList,cpu_sys,'Time(secs)','System',
                 timeList,cpu_idle,'Time(secs)','Idle',timeList,cpu_iowait,'Time(secs)','IO_Wait',
                 output_dir+'/'+host+'_cpu_stats')        
###############################################################################################################
def iostat_graph(s,delay,host,fp,output_dir):
        '''
                s is a string with the monitored host's vmstat output
                fp is either a file object (opened) or None
                rest vars are self-explanatory
        '''

        l = filter(None,re.sub(r'[ \t\r\f\v]+',' ',s).split('\n'))      # iostat's contents, with single spaces, split at newlines
        l[:] = [i for i in l if len(i) > 2 ]                            # remove single \n items (in place)

        deviceLines = [i for i, x in enumerate(l) if "Device:" in x]    #lines containing "Device:" string (each such line designates new output)
        diffs = [j-i for i,j in zip(deviceLines[:-1],deviceLines[1:])]
        if len(set(diffs)) != 1:
                print "Error while parsing iostat output. Wrong diffs found. Will not plot graphs."
                return

        deviceNum = diffs[0]-1

        deviceNames = []
        for i in l[deviceLines[0]+1:deviceLines[1]]: #not deviceLines[1]-1, due to 0-based indexing ;-)
                deviceNames.append(i.split(' ')[0])

        samples=len(deviceLines)
        timeList = [ (x*y) for x in range(1,samples+1) for y in [int(delay)]] #samples+1 just in case

        for name in deviceNames:
                readKB  = []
                writeKB = []

                for line in l:
                        if name in line:
                                tmp = line.split(' ')
                                readKB.append(tmp[2])
                                writeKB.append(tmp[3])

                if fp:
                        fp.write('##:'+host+':'+name+':readKB'+'\n')
                        fp.write(repr(zip(timeList,readKB))+'\n')
                        fp.write('##:'+host+':'+name+':writeKB'+'\n')
                        fp.write(repr(zip(timeList,writeKB))+'\n')

                do_plot2(timeList,readKB,'Time(secs)','Read(KB)',timeList,writeKB,"Time(secs)",
                         "Write(KB)",output_dir+'/'+host+'_'+name)
###############################################################################################################
def qstat_graph(s,delay,host,fp,output_dir):
        '''
                s contains one number in each line, representing the sum of the queued jobs in torque
                fp is either a file object (opened) or None
                rest vars are self-explanatory
        '''

        l = filter(None,re.sub(r'[ \t\r\f\v]+',' ',s).split('\n'))              # qstat's contents, without whitespaces, split at newlines
        l[:] = [i for i in l if len(i) > 1 ]                                    # remove empty items (in place)
        timeList = [ (x*y) for x in range(1,len(l)+1) for y in [int(delay)]]    #len(l)+1 just in case

        if fp:
                fp.write('##:'+host+':qstat'+'\n')
                fp.write(repr(zip(timeList,l))+'\n')

        do_plot1(timeList,l,'Time(secs)','Queued_Jobs',output_dir+'/'+host+'_qstat')
###############################################################################################################
def do_plot1(xList, yList, xLabel, yLabel, name):
        '''
                Graph a plot of one set of variables
                Note: backend is allready set @ the import section
        '''

        fig = plt.figure()
        ax = fig.gca()                                                          # get axes
        ax.plot(xList[:len(yList)], yList, label=yLabel)                        # slice in case some x values are missing and plot the data
        ax.grid()                                                               # show grids
        ax.yaxis.set_major_formatter(ScalarFormatter(useOffset=False))          # turn off tampering with my data!
        plt.xlabel(xLabel)
        plt.ylabel(yLabel)
        plt.title(name)
        plt.legend(prop={'size':9})
        plt.savefig(name+'_'+yLabel+'.png')
###############################################################################################################
def do_plot2(x1List, y1List, x1Label, y1Label, x2List, y2List, x2Label, y2Label, name):
        '''
                Graph a plot of two sets of variables
                Note: backend is allready set @ the import section
        '''

        fig = plt.figure()
        ax = fig.gca()                                                          # get axes
        ax.plot(x1List[:len(y1List)], y1List, label=y1Label)                    # slice in case some x values are missing and plot the data
        ax.plot(x2List[:len(y2List)], y2List, label=y2Label)                    # slice in case some x values are missing and plot the data
        ax.grid()                                                               # show grids
        ax.yaxis.set_major_formatter(ScalarFormatter(useOffset=False))          # turn off tampering with my data!
        plt.xlabel(x1Label)  #time is usualy X-axis, so just print x1 label
        plt.ylabel(y1Label+'/'+y2Label)
        plt.title(name)
        plt.legend(prop={'size':9})
        plt.savefig(name+'_'+y1Label+'_'+y2Label+'.png')
###############################################################################################################
def do_plot4(x1L, y1L, x1Lbl, y1Lbl, x2L, y2L, x2Lbl, y2Lbl, x3L, y3L, x3Lbl, y3Lbl, x4L, y4L, x4Lbl, y4Lbl, name):
        '''
                Graph a plot of four sets of variables
                Note: backend is allready set @ the import section
        '''

        fig = plt.figure()
        ax = fig.gca()                                                          # get axes
        ax.plot(x1L[:len(y1L)], y1L, label=y1Lbl)                               # slice in case some x values are missing and plot the data
        ax.plot(x2L[:len(y2L)], y2L, label=y2Lbl)                               # slice in case some x values are missing and plot the data
        ax.plot(x3L[:len(y3L)], y3L, label=y3Lbl)                               # slice in case some x values are missing and plot the data
        ax.plot(x4L[:len(y4L)], y4L, label=y4Lbl)                               # slice in case some x values are missing and plot the data
        ax.grid()                                                               # show grids
        ax.yaxis.set_major_formatter(ScalarFormatter(useOffset=False))          # turn off tampering with my data!
        plt.xlabel(x1Lbl) #time is usualy X-axis, so just print x1 label
        plt.ylabel(y1Lbl+'/'+y2Lbl+'/'+y3Lbl+'/'+y4Lbl)
        plt.title(name)
        plt.legend(prop={'size':9})
        plt.savefig(name+'_'+y1Lbl+'_'+y2Lbl+'_'+y3Lbl+'_'+y4Lbl+'.png')
###############################################################################################################
def check_com_existence(host,user,port,command):
        '''
                guess what!
        '''

        return _enisc(command + ' ; echo ":$?:"',host,user,port)
###############################################################################################################
def f(wl,delay,savestats,q,output_dir):
        '''
                Run monitor script on the given host, gather the produced data, plot them and erase leftover files.
                Also, optionally, save the ploted data in parse-friendly format.
                wl        : dict with keys: host(str),user(str),port(int),progs(str),procs(str)
                delay     : int 
                savestats : True or False 
                q         : a sync queue object for message passing
        '''

        # Create file to store statistics, if requested
        if savestats == True:
                name = 'cream_stress_test_monitor_script_statistics_' +\
                       str(time.time()) + '_' + str(os.getpid()) + '.dat'
                path = '/tmp/' + name
                fp = open(path,'w')
                print "Monitoring data saved as: " + path
        else:
                fp=None

        # Check if the needed executables exist
        s=check_com_existence(wl['host'],wl['user'],wl['port'],'which scp')
        if ':0:' not in s:
                raise _error("Scp command not found on host:" + wl['host'] + ". Aborting!")
        s=check_com_existence(wl['host'],wl['user'],wl['port'],'which killall')
        if ':0:' not in s:
                raise _error("Scp command not found on host:" + wl['host'] + ". Aborting!")

        if 'iostat' in wl['progs']:
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'iostat')
                if ':0:' not in s:
                        raise _error("Iostat command not found on host:" + wl['host'] + ". Aborting!")
        if 'vmstat' in wl['progs']:
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'vmstat')
                if ':0:' not in s:
                        raise _error("Vmstat command not found on host:" + wl['host'] + ". Aborting!")
        if 'sar' in wl['progs']:
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'sar')
                if ':0:' not in s:
                        raise _error("Sar command not found on host:" + wl['host'] + ". Aborting!")
        if 'qstat' in wl['progs']:
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'qstat')
                if ':0:' not in s:
                        raise _error("Qstat command not found on host:" + wl['host'] + ". Aborting!")
        if len(wl['procs'].split(',')) > 0:
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'ps')
                if ':0:' not in s:
                        raise _error("Ps command not found on host:" + wl['host'] + ". Aborting!")

        # Create monitoring script file
        script_fpath = create_script(iostat = True if 'iostat' in wl['progs'] else False,
                                     vmstat = True if 'vmstat' in wl['progs'] else False,
                                     sar    = True if 'sar'    in wl['progs'] else False,
                                     qstat  = True if 'qstat'  in wl['progs'] else False,
                                     ps=wl['procs'],
                                     delay=int(delay),
                                     output_dir='/tmp')
        script_name = script_fpath.split('/')[-1]

        # Copy the monitoring script (executed in bash -c, cause pexpec.run() passes
        # everything as a literal argument, thus the error-checking echo couldn't execute.
        scp_com = 'scp -P ' + str(wl['port']) + ' ' + script_fpath + ' ' + wl['user'] + '@' + wl['host'] + ':.'
        scp_com += ' ; echo ":$?:"'
        retVal=pexpect.run ('bash -c "' +scp_com+'"')
        if ':0:' not in retVal:
                raise _error("Scp command:\n" + scp_com + "\nhas failed."+\
                             "Command reported:" + retVal + "Aborting!")

        # Execute the copied script
        _enisc('chmod +x ' + script_name, wl['host'], wl['user'], wl['port'])                   #make script executable
        signal.signal(signal.SIGCHLD, signal.SIG_IGN) #leave it to the kernel to reap my dead children (non-posix, not portable!)
        com = 'ssh -p ' + str(wl['port']) + ' ' + wl['user'] + '@' + wl['host'] +\
              ' "echo \"authenticated:\" ; ./' + script_name + '" &'
        _senc(com)                                                                              #run it in the background

        arr = q.remove()        #wait to receive "signal" to stop monitoring (queue's contents are irrelevant)

        signal.signal(signal.SIGCHLD, signal.SIG_DFL) # reset signal handler to default

        _enisc('killall iostat vmstat sar ' + script_name, wl['host'],wl['user'],wl['port'])

        # Gather the produced files data
        if 'iostat' in wl['progs']:
                iostat_contents = _enisc('cat iostat.dat',wl['host'],wl['user'],wl['port'])
        if 'vmstat' in wl['progs']:
                vmstat_contents = _enisc('cat vmstat.dat',wl['host'],wl['user'],wl['port'])
        if 'sar' in wl['progs']:
                sar_contents = _enisc('cat sar.dat',wl['host'],wl['user'],wl['port'])
        if 'qstat' in wl['progs']:
                qstat_contents = _enisc('cat qstat.dat',wl['host'],wl['user'],wl['port'])
        if len(wl['procs'].split(',')) > 0:
                ps_contents = []
                for com in wl['procs'].split(','):
                        ps_contents.append(':'+com+':\n' + _enisc('cat '+com+'.dat',wl['host'],wl['user'],wl['port']))

        # Delete leftover files
        _enisc('rm -f *dat',wl['host'],wl['user'],wl['port'])
        _enisc('rm -f ' + script_name,wl['host'],wl['user'],wl['port'])

        # Plot graphs
        if 'iostat' in wl['progs']:
                iostat_graph(iostat_contents,int(delay),wl['host'],fp,output_dir)
        if 'vmstat' in wl['progs']:
                vmstat_graph(vmstat_contents,int(delay),wl['host'],fp,output_dir)
        if 'sar' in wl['progs']:
                sar_graph(sar_contents,int(delay),wl['host'],fp,output_dir)
        if 'qstat' in wl['progs']:
                qstat_graph(qstat_contents,int(delay),wl['host'],fp,output_dir)
        if len(wl['procs'].split(',')) > 0:
                ps_graph(ps_contents,int(delay),wl['host'],fp,output_dir)

        # Close statistics file, if needed
        if fp:
                fp.close()
###############################################################################################################
def usage():
    print "USAGE"
    print "\n To execute the program, run:\n\t\t" + sys.argv[0] + ' path/to/config/file.ini'
    print "\n To get help, run:\n\t\t" + sys.argv[0] + ' --help'
###############################################################################################################
def help():
    print "This is a script which automatically monitors ssh enabled hosts and provides plots and statistics"+\
          " as a result. For this process it uses the following programs, which must be available on the monitored"+\
          " host:"
    print "vmstat: for memory statistics"
    print "iostat: for io statistics"
    print "sar:    for network statistics"
    print "qstat:  for torque queue statistics"
    print "ps:     for process statistics (cpu, memory)"
    print "Please note that there must be automatic passwordless key authentication set up between the monitoring"+\
          " and the monitored host, prior to the start of the monitoring operation."
    print
    print "The script takes as an argument the path to a python configuration file, which describes which hosts should "+\
          " be monitored and how."
    print "This config file is based on the Python's ConfigParser module. You can find documentation on said "+\
          "module here: http://docs.python.org/2/library/configparser.html"
    print "The format of the file is as follows:"
    print "\t[main]"
    print "\tdelay="
    print "\tsavestats="
    print
    print "\t[hosts]"
    print "\tkeys=host1,host2,..."
    print
    print "\t[host1]"
    print "\thost="
    print "\tuser="
    print "\tport="
    print "\tprogs="
    print "\tprocs"
    print
    print "\t[host2]"
    print "\t..."
    print "\t..."
    print
    print "Variable description:"
    print "\tdelay\n\t\tThe delay between monitor operations (argument for iostat, vmstat, sar etc)"
    print "\t\tValue: integer in the range 1-30"
    print "\tsavestats\n\t\tIf set, the statistics will be saved in a file in a -parse- friendly format."
    print "\t\tValue: True or False"
    print "\toutput_dir\n\t\tThe directory in which to put produced graphs."
    print "\t\tValue: any valid path, defaults to the current working directory."
    print "\tkeys\n\t\tThe (arbitrary) names of the following sections describing hosts to be monitored."
    print "\t\tValue: comma separated string"
    print "\thost\n\t\tThe host name of the host being monitored."
    print "\t\tValue: any valid hostname"
    print "\tuser\n\t\tThe user to utilize for monitoring. He must have ssh access to the monitored host and be able "+\
          "to execute the monitoring programs (see bellow)."
    print "\t\tValue: any valid user name."
    print "\tport\n\t\tthe ssh port for the specified host."
    print "\t\tValue: integer in the range 1-65535"
    print "\tprogs\n\t\tWhich monitoring programs to run on the specified host."
    print "\t\tValue: comma separated list including any of the following: vmstat, iostat, sar, qstat"
    print "\tprocs\n\t\tprocesses to monitor on the specified host"
    print "\t\tValue: comma separated list of process names running on the specified host. Non existing processes are ignored."
    print
    print "Example configuration file:"
    print "\t[main]"
    print "\tdelay=3"
    print "\tsavestats=True"
    print
    print "\t[hosts]"
    print "\tkeys=cream,torque,mysql"
    print
    print "\t[cream]"
    print "\thost=cream.mydomain.org"
    print "\tuser=root"
    print "\tport=22"
    print "\tprogs=vmstat,iostat,sar"
    print "\tprocs=java,BUpdaterPBS,BNotifier"
    print
    print "\t[mysql]"
    print "\thost=db.mydomain.org"
    print "\tuser=root"
    print "\tport=22"
    print "\tprogs=vmstat,iostat,sar"
    print "\tprocs=mysql"
    print
    print "\t[torque]"
    print "\thost=lrms.mydomain.org"
    print "\tuser=root"
    print "\tport=22"
    print "\tprogs=vmstat,iostat,sar,qstat"
    print "\tprocs=pbs_server,maui,munged"
###############################################################################################################
def check_args(l):
    '''
        Check sys.argv arguments of the script
    '''

    if len(l) != 2:
        usage()
        sys.exit(1)
    elif l[1] == "--help" or l[1] == "-h":
        help()
        sys.exit(0)
    elif not os.path.isfile(l[1]):
        print "Configuration file: " + l[1] + " doesn't exist or isn't a normal file."
        usage()
        sys.exit(1)
###############################################################################################################
def parse_arguments(cfg_file):
        '''
                Command line configuration file parsing routine
        '''

        args = {}

        cfg = ConfigParser.SafeConfigParser({'output_dir':'./'})
        cfg.read(cfg_file)

        args['delay']=cfg.getint('main','delay')
        if args['delay'] < 1 or args['delay'] > 30:
            raise _error('Argument delay should be between 1 and 30, is:' + str(args['delay']))

        args['savestats']=cfg.getboolean('main','savestats')
        if args['savestats'] != True and args['savestats'] != False:
            raise _error('Argument savestats should be True or False, is:' + str(args['savestats']))

        args['output_dir']=cfg.get('main','output_dir')
        if len(args['output_dir'].strip()) == 0:
            args['output_dir']="./"
        if not os.path.isdir(args['output_dir']):
            args['output_dir']="./"

        args['hosts']=cfg.get('hosts','keys')
        if len(args['hosts'].strip()) == 0:
            raise _error('Argument hosts (keys) should not be empty')

        for host in [x.strip() for x in args['hosts'].split(',')]:
            tmp_dict = {}
            tmp_dict['host']=cfg.get(host,'host')
            if len(tmp_dict['host'].strip()) == 0:
                raise _error('Argument host for host:' + host + ' should not be empty.')

            tmp_dict['user']=cfg.get(host,'user')
            if len(tmp_dict['user'].strip()) == 0:
                raise _error('Argument user for host:' + host + ' should not be empty.')

            tmp_dict['port']=cfg.getint(host,'port')
            if tmp_dict['port'] < 1 or tmp_dict['port'] > 65535:
                raise _error('Argument port should be between 1 and 65535, is:' + str(tmp_dict['port']))

            tmp_dict['progs']=cfg.get(host,'progs')
            if len(tmp_dict['progs'].strip()) == 0:
                raise _error('Argument progs for host:' + host + ' should not be empty.')
            for prog in [x.strip() for x in tmp_dict['progs'].split(',')]:
                if prog not in ['iostat','qstat','vmstat','sar']:
                    raise _error('Argument progs should contain any combination of the values iostat'+\
                                 ', qstat, vmstat, sar. Offending value:' + prog)

            #this value can be empty
            tmp_dict['procs']=cfg.get(host,'procs')

            args[host]=tmp_dict

        return args
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################


if __name__ == '__main__':
        '''
                the main function
        '''

        check_args(sys.argv)
        args = parse_arguments(sys.argv[1])

        numProcs = len(args['hosts'].split(','))
        q = sync_queue()

        start = datetime.datetime.now()
        print 'Starting at: ' + start.strftime('%c') + '\n\n\n\n'

        procs = []
        #for num in range(numProcs):
        for host in [x.strip() for x in args['hosts'].split(',')]:      # the monitoring procs, one for each host
                p=Process(target=f, args=(args[host],args['delay'],args['savestats'],q,args['output_dir']))
                procs.append(p)
                p.start()

        print "Press Enter anytime you want to terminate monitoring"
        raw_input("\n")
        for i in range(len(args['hosts'].split(','))):
                q.add('AEK')

        proc_list = multiprocessing.active_children()
        while ( len(proc_list) > 0 ):
            proc_list = multiprocessing.active_children()
            time.sleep(5) #non-busy wait

        end = datetime.datetime.now()
        print '\nEnded at: ' + end.strftime('%c')
        elapsed = end - start
        print 'Time elapsed: ' + str(elapsed)
