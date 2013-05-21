#!/usr/bin/env python

# Copyright 2012 Dimosthenes Fioretos dfiore -at- noc -dot- uoa -dot- gr
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
# Version       |   1.0                                                                                 #
#-------------------------------------------------------------------------------------------------------#
# Dependencies  |   pexpect                                                                             #
#               |   python-matplotlib                                                                   #
#               |   python-argparse                                                                     #
#-------------------------------------------------------------------------------------------------------#
# Invocation    |   Run script with -h argument                                                         #
#-------------------------------------------------------------------------------------------------------#
#    Notes      |                                                                                       #
#########################################################################################################


from multiprocessing import Process, Lock, Queue, queues
import multiprocessing
import pexpect, argparse, re, sys, time, subprocess , shlex, os, signal, math, datetime
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
                #with self.lock:                #"with" supports lock objects automagically
                        #self.q.put(item)       # dropped it for traditional acquire()/release(), for readability mainly.

                self.lock.acquire()
                #print 'Putting in queue: ' + str(item)
                self.q.put(item)
                self.lock.release()

        def remove(self):
                return self.q.get()
###############################################################################################################
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
        if iostat == 'True':
                cnts =  cnts +\
                        'iostat -k -d '+str(delay)+'  > iostat.dat &\n'
        if vmstat == 'True':
                cnts =  cnts +\
                        'vmstat -w -n '+str(delay)+'  > vmstat.dat &\n'
        if sar == 'True':
                cnts =  cnts +\
                        'sar -n DEV '+str(delay)+'  > sar.dat &\n'

        #if len(ps) > 0 or qstat == 'True':
        if ps != False or qstat == 'True':
                cnts =  cnts +\
                        '\nwhile true\n'+\
                        'do\n'
                if ps != False:
                        for proc in ps.split(','):
                                cnts =  cnts +\
                                        '\tps -C '+proc+' --no-headers -o %cpu,size >> '+proc+'.dat\n'
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
                 Returns:         the command's output                                                                               
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

        #print 'Command: "' + command + '"'
        #print "SSH Command:" + ssh_com

        child = pexpect.spawn(ssh_com, timeout=604800) #wait for 7 days (lol) at most for each command to finish
        index = child.expect([expect_key, expect_pass, expect_eof, expect_auth])
        if index == ikey:
                print 'Added foreign host key fingerprint...'
                child.sendline('yes')
                child.sendeof()
                index = child.expect([expect_key, expect_pass, expect_eof, expect_auth])
                if index == ipasswd:
                        raise _error('Pexpect couldn\'t find a proper or right match for "' + ssh_com + '" in "' + \
                                      expect_key + '" "' + expect_pass + '" "EOF"')
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
                raise _error('Pexpect couldn\'t find a proper or right match for "' + ssh_com + '" in "' + \
                              expect_key + '" "' + expect_pass + '" "EOF"')
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
        #print 'Output: ' + retVal
        return retVal
###############################################################################################################
###############################################################################################################
def _enisc(command,host,user,port=22,background=False):
        # short name wrapper-wannabe. I hate long function names :P
        return execute_noninteractive_ssh_com(command,host,user,port,background)
###############################################################################################################
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
###############################################################################################################
def _senc(com,wait=False):
        # wrapper
        return sub_execute_noninteractive_com(com,wait)
###############################################################################################################
###############################################################################################################
def ps_graph(l,delay,host,fp):
        '''
                l is a list with the monitored proc's name @ its 1st line and then a duet of numbers (cpu,size)
                @ each line after that, one each delay seconds.
                host used to name uniquely and meaningfully the plot files
        '''

        samples=0
        for i in l[0].split('\n'):
                tmp1=re.sub(r'\s', '', i)       # replase (actually erase) whitespaces...
                tmp2=re.sub(r'\.', '', tmp1)    # ...and dots (used in real number's representation)
                if tmp2.isdigit():
                        samples += 1

        timeList = [ (x*y) for x in range(1,samples+1) for y in [int(delay)]]

        for proc in l:
                name=proc.split(':')[1]

                tmp1 = re.sub(r'\s', ' ', proc.split(':')[2])     # erase whitespace characters
                tmp2 = tmp1.split(' ')                            # split items
                vals = filter(None,tmp2)                          # remove empty items

                cpu  = vals[0::2]                                 # take every 2nd item, starting at 1st item (note: 0-based indexing)
                size = vals[1::2]                                 # take every 2nd item, starting at 2nd item (note: 0-based indexing)

                sizeKB = [round(float(x)/1024.0,2) for x in size] # memory size in KB rounded to 2 decimal points for better visualization

                if fp:
                        fp.write('##:'+host+':'+name+':cpu'+'\n')
                        fp.write(repr(zip(timeList,cpu))+'\n')
                        fp.write('##:'+host+':'+name+':size'+'\n')
                        fp.write(repr(zip(timeList,sizeKB))+'\n')


                # plot it
                do_plot1(timeList,cpu,'Time(secs)','CPU',host+'_'+name)
                do_plot1(timeList,sizeKB,'Time(secs)','Mem.Size(KB)',host+'_'+name) #convert bytes to KB for mem.size
###############################################################################################################
###############################################################################################################
def sar_graph(s,delay,host,fp):
        '''
                s is a string with the monitored host's sar -n DEV output
                fp is either a file object (opened) or None
                rest vars are self-explanatory
        '''

        l = filter(None,re.sub(r'[ \t\r\f\v]+',' ',s).split('\n'))      # sar's contents, with single spaces, split at newlines
        l[:] = [i for i in l if len(i) > 2 ]                              # remove single \n items (in place)

        ifaceLines = [i for i, x in enumerate(l) if "IFACE" in x]
        diffs = [j-i for i,j in zip(ifaceLines[:-1],ifaceLines[1:])]
        if len(set(diffs)) != 1:
                print "Error while parsing sar output. Wrong diffs found. Will not plot graphs."
                return
        ifaceNum = diffs[0]-1

        ifaceNames = []
        for i in l[ifaceLines[0]+1:ifaceLines[1]]: #not ifaceLines[1]-1, due to 0-based indexing ;-)
                ifaceNames.append(i.split(' ')[2])


        samples=0
        for i in l:
                if "IFACE" in i:
                        samples += 1
        timeList = [ (x*y) for x in range(1,samples+1) for y in [int(delay)]]

        for name in ifaceNames:
                rx = []
                tx = []

                for line in l:
                        if name in line:
                                tmp = line.split(' ')
                                rx.append(tmp[5])
                                tx.append(tmp[6])


                if fp:
                        fp.write('##:'+host+':'+name+':transmit'+'\n')
                        fp.write(repr(zip(timeList,rx))+'\n')
                        fp.write('##:'+host+':'+name+':recieve'+'\n')
                        fp.write(repr(zip(timeList,tx))+'\n')

                do_plot2(timeList,rx,'Time(secs)','Receive(KBps)',timeList,tx,"Time(secs)","Transmit(KBps)",host+'_'+name)
###############################################################################################################
###############################################################################################################
def vmstat_graph(s,delay,host,fp):
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

        do_plot2(timeList,procs_ready,'Time(secs)','Ready Procs',timeList,procs_sleep,'Time(secs)','Sleeping Procs',host+'_processes')
        do_plot4(timeList,mem_swap,'Time(secs)','Swap',timeList,mem_free,'Time(secs)','Free',
                 timeList,mem_buff,'Time(secs)','Buffers',timeList,mem_cache,'Time(secs)','Cache',host+'_mem_stats')
        do_plot2(timeList,sys_ips,'Time(secs)','Interrupts',timeList,sys_csps,'Time(secs)','Context_Switches (both per second)',host+'_system_stats')
        do_plot4(timeList,cpu_user,'Time(secs)','User',timeList,cpu_sys,'Time(secs)','System',
                 timeList,cpu_idle,'Time(secs)','Idle',timeList,cpu_iowait,'Time(secs)','IO_Wait',host+'_cpu_stats')        
###############################################################################################################
###############################################################################################################
def iostat_graph(s,delay,host,fp):
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

                do_plot2(timeList,readKB,'Time(secs)','Read(KB)',timeList,writeKB,"Time(secs)","Write(KB)",host+'_'+name)
###############################################################################################################
###############################################################################################################
def qstat_graph(s,delay,host,fp):
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

        do_plot1(timeList,l,'Time(secs)','Queued_Jobs',host+'_qstat')
###############################################################################################################
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
###############################################################################################################
def check_com_existence(host,user,port,command):
        '''
                guess what!
        '''

        return _enisc(command + ' ; echo ":$?:"',host,user,port)
###############################################################################################################
###############################################################################################################
def lod(listDict):
        '''
                Parse command line given argument.
                Read -h output to understand the syntax.
                (finally: list of dictionaries of the form: {user,host,port,commands_to_run,procs_to_monitor})
        '''

        final = []
        l = listDict.split(':')
        try:
                for item in l:
                        ssh_args = item.split('{')[0]
                        coms = item.split('{')[1].split('}')[0]
                        progs = None
                        if '[' in item and ']' in item:
                                progs = item.split('[')[1].split(']')[0]

                        d={"user":"foo", "host":"bar", "port":"zap", "iostat":"False",
                           "vmstat":"False", "sar":"False", "ps":"False", "qstat":"False"}

                        d['user']=ssh_args.split(',')[0]
                        d['host']=ssh_args.split(',')[1]
                        d['port']=ssh_args.split(',')[2]

                        for i in coms.split(','):
                                if i == 'vmstat':
                                        d['vmstat']='True'
                                elif i == 'iostat':
                                        d['iostat']='True'
                                elif i == 'sar':
                                        d['sar']='True'
                                elif i == 'qstat':
                                        d['qstat']='True'
                                else:
                                        raise _error('Unknown monitoring command given')

                        if progs is not None:
                                d['ps']=progs

                        final.append(d)
        except Exception as e:
                #import traceback
                #traceback.print_exc()
                msg = 'Argument parsing error! It must be in the form: user,host,port{commands}[procs]:...\n'+\
                      'Where commands and procs should be like: {vmstat,iostat,sar,qstat}[java,BUpdater]\n'+\
                      'NOTE: Arguments should always be passed inside single quotes \"\'\" !!!'+\
                      'Specific error returned:' + str(e)
                raise argparse.ArgumentTypeError(msg)

        return final
###############################################################################################################
###############################################################################################################
def parse_arguments():
        '''
                Command line argument parsing routine
        '''

        parser = argparse.ArgumentParser(description = 'Collect statistics for given hosts.',
                                         epilog      = 'For help, bugs, etc contact the author at dfiore -at- noc -dot- edunet -dot- gr')
        parser.add_argument('watchlist',
                            help = 'The watchlist must be in the form: user,host,port{commands}[procs]:...\n'+\
                                   'Where "commands" and "procs" should be like: {vmstat,iostat,sar,qstat}[java,BUpdater]\n'+\
                                   'The only options for the "commands" part, are the ones given\n'+\
                                   'The "procs" command can contain any process running in the target host\n'+\
                                   'The user,host,port and "commands" arguments are mandatory, the "procs" is optional\n'+\
                                   'The brackets "[","]" and curly brackets "{","}" MUST be used'
                                   'The watchlist must ALWAYS be passed inside single quotes \"\'\" !!!'+\
                                   'Example for one host: system_stats2.py \'root,cream.uoa.gr,22{vmstat,iostat,sar}[java,BUpdaterPBS,BNotifier]\' -d 5 -s'+\
                                   'Example for multiple hosts: system_stats2.py \'root,cream.uoa.gr,22{vmstat,iostat,sar}[java,BUpdaterPBS,BNotifier]:root,db.uoa.gr,22{vmstat,iostat,sar}[mysqld]:root,lrms.uoa.gr,22{vmstat,iostat,sar,qstat}[pbs_server,maui,munged]\' -d 5 -s',
                           type=lod)
        parser.add_argument('-d','--delay',
                            required=True,
                            help='The delay between monitor operations (argument for iostat, vmstat, sar etc)',
                            type=int,
                            dest='delay')
        parser.add_argument('-s','--savestats',
               help='If set, the statistics will be saved in a file in a -parse- friendly format.',
               action='store_true',
               dest='savestats')
        args = parser.parse_args()
        return args

###############################################################################################################
###############################################################################################################
def f(wl,delay,savestats,q):
        '''
                Run monitor script on the given host, gather the produced data, plot them and erase leftover files.
                Also, optionally, save the ploted data in parse-friendly format.
                wl        : watchlist for single host, check -h text also 
                delay     : int 
                savestats : True or False 
                q         : a sync queue object for message passing
        '''

        # Create file to store statistics, if requested
        if savestats == True:
                name = 'cream_stress_test_monitor_script_statistics_' + str(time.time()) + '_' + str(os.getpid()) + '.dat'
                path = '/tmp/' + name
                fp = open(path,'w')
                print "Monitoring data saved as: " + path
        else:
                fp=None

        # Check if the needed executables exist
        if wl['iostat'] == 'True':
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'iostat')
                if ':0:' not in s:
                        raise _error("Iostat command not found on host:" + wl['host'] + ". Aborting!")
        if wl['vmstat'] == 'True':
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'vmstat')
                if ':0:' not in s:
                        raise _error("Vmstat command not found on host:" + wl['host'] + ". Aborting!")
        if wl['sar'] == 'True':
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'sar')
                if ':0:' not in s:
                        raise _error("Sar command not found on host:" + wl['host'] + ". Aborting!")
        if wl['qstat'] == 'True':
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'qstat')
                if ':0:' not in s:
                        raise _error("Qstat command not found on host:" + wl['host'] + ". Aborting!")
        if wl['ps'] != 'False':
                s=check_com_existence(wl['host'],wl['user'],wl['port'],'ps')
                if ':0:' not in s:
                        raise _error("Ps command not found on host:" + wl['host'] + ". Aborting!")

        # Create monitoring script file
        script_fpath = create_script(iostat=wl['iostat'],vmstat=wl['vmstat'],sar=wl['sar'],qstat=wl['qstat'],
                                     ps=wl['ps'],delay=int(delay),output_dir='/tmp')
        script_name = script_fpath.split('/')[-1]

        # Copy and execute the monitoring script
        scp_com = 'scp ' + script_fpath + ' ' + wl['user'] + '@' + wl['host'] + ':.'
        pexpect.run (scp_com)                                                                   #copy over the monitor script
        _enisc('chmod +x ' + script_name, wl['host'], wl['user'], wl['port'])                   #make script executable
        signal.signal(signal.SIGCHLD, signal.SIG_IGN) #leave it to the kernel to reap my dead children (non-posix, not portable!)
        com = 'ssh -p ' + wl['port'] + ' ' + wl['user'] + '@' + wl['host'] +\
              ' "echo \"authenticated:\" ; ./' + script_name + '" &'
        _senc(com)                                                                              #run it in the background

        arr = q.remove()        #wait to receive "signal" to stop monitoring (queues contents are irrelevant)

        signal.signal(signal.SIGCHLD, signal.SIG_DFL) # reset signal handler to default

        _enisc('killall iostat vmstat sar ' + script_name, wl['host'],wl['user'],wl['port'])

        # Gather the produced files data
        if wl['iostat'] == 'True':
                iostat_contents = _enisc('cat iostat.dat',wl['host'],wl['user'],wl['port'])
        if wl['vmstat'] == 'True':
                vmstat_contents = _enisc('cat vmstat.dat',wl['host'],wl['user'],wl['port'])
        if wl['sar'] == 'True':
                sar_contents = _enisc('cat sar.dat',wl['host'],wl['user'],wl['port'])
        if wl['qstat'] == 'True':
                qstat_contents = _enisc('cat qstat.dat',wl['host'],wl['user'],wl['port'])
        if wl['ps'] != 'False':
                #print 'wl[\'ps\']=' + repr(wl['ps'])
                ps_contents = []
                for com in wl['ps'].split(','):
                        ps_contents.append(':'+com+':\n' + _enisc('cat '+com+'.dat',wl['host'],wl['user'],wl['port']))

        # Delete leftover files
        _enisc('rm -f *dat',wl['host'],wl['user'],wl['port'])
        _enisc('rm -f ' + script_name,wl['host'],wl['user'],wl['port'])

        # Plot graphs
        if wl['iostat'] == 'True':
                iostat_graph(iostat_contents,int(delay),wl['host'],fp)
        if wl['vmstat'] == 'True':
                vmstat_graph(vmstat_contents,int(delay),wl['host'],fp)
        if wl['sar'] == 'True':
                sar_graph(sar_contents,int(delay),wl['host'],fp)
        if wl['qstat'] == 'True':
                qstat_graph(qstat_contents,int(delay),wl['host'],fp)
        if wl['ps'] != 'False':
                ps_graph(ps_contents,int(delay),wl['host'],fp)

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
    print "\t\tDefault: 5"
    print "\tsavestats\n\t\tIf set, the statistics will be saved in a file in a -parse- friendly format."
    print "\t\tValue: True or False"
    print "\t\tDefault: False"
    print "\tkeys\n\t\tThe (arbitrary) names of the following sections describing hosts to be monitored."
    print "\t\tValue: comma separated string"
    print "\t\tDefault: n/a"
    print "\thost\n\t\tThe host name of the host being monitored."
    print "\t\tValue: any valid hostname"
    print "\t\tDefault: n/a"
    print "\tuser\n\t\tThe user to utilize for monitoring. He must have ssh access to the monitored host and be able "+\
          "to execute the monitoring programs (see bellow)."
    print "\t\tValue: any valid user name."
    print "\t\tDefault: n/a"
    print "\tport\n\t\tthe ssh port for the specified host."
    print "\t\tValue: integer in the range 1-65535"
    print "\t\tDefault: 22"
    print "\tprogs\n\t\tWhich monitoring programs to run on the specified host."
    print "\t\tValue: comma separated list including any of the following: vmstat, iostat, sar, qstat"
    print "\t\tDefault: n/a"
    print "\tprocs\n\t\tprocesses to monitor on the specified host"
    print "\t\tValue: comma separated list of process names running on the specified host. Non existing processes are ignored."
    print "\t\tDefault: n/a"
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
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################


if __name__ == '__main__':
        '''
                the main function
        '''

        if len(sys.argv) != 2:
            usage()
            sys.exit(1)
        elif sys.argv[1] == "--help" or sys.argv[1] == "-h":
            help()
            sys.exit(0)

        args = parse_arguments()
        numProcs = len(args.watchlist)
        q = sync_queue()

        start = datetime.datetime.now()
        print 'Starting at: ' + start.strftime('%c') + '\n\n\n\n'

        procs = []
        for num in range(numProcs):                # the monitoring procs, one for each host
                p=Process(target=f, args=(args.watchlist[num],args.delay,args.savestats,q))
                procs.append(p)
                p.start()

        print "Press Enter anytime you want to terminate monitoring"
        raw_input("\n")
        for i in range(numProcs):
                q.add('AEK')

        proc_list = multiprocessing.active_children()
        while ( len(proc_list) > 0 ):
            proc_list = multiprocessing.active_children()
            time.sleep(5) #non-busy wait

        end = datetime.datetime.now()
        print '\nEnded at: ' + end.strftime('%c')
        elapsed = end - start
        print 'Time elapsed: ' + str(elapsed)
