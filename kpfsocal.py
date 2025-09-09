import datetime as dt
import os
import subprocess as sp

def kpfsocal(utDate=None, dir='.', log_writer=None):
    '''
    Retrieve KPF SoCal data

    @type utDate: string
    @param utDate: UT date of data to retrieve (default is current UT date)
    @type dir: string
    @param dir: Directory to write data to (default is current directory)
    '''

    if log_writer:
        log_writer.info(f'kpfsocal.py started for {utDate}')

    # If no utDate supplied, use the current value

    if not utDate:
        utDate = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d')

    assert dt.datetime.strptime(utDate, '%Y-%m-%d')

    # Need to grab 2 days of data since in HST

    start  = dt.datetime.strptime(f"{utDate} 14:00:00", '%Y-%m-%d %H:%M:%S')
    end    = start + dt.timedelta(days=1)
    start  = start - dt.timedelta(days=1)

    # gshow command

    cmd = []
    cmd.append("/kroot/rel/default/bin/gshow")
    cmd.append("-s")
    cmd.append("kpfsocal")
    cmd.append("pyrirrad")
    cmd.append("--date")
    cmd.append(start.strftime("%Y-%m-%d"))
    cmd.append("--date")
    cmd.append(end.strftime("%Y-%m-%d"))
    cmd.append("-csv")

    # Construct file to write to

    writeFile = f'{dir}/kpfsocal.csv'

    # Get data

    if log_writer:
        log_writer.info(f'kpfsocal.py gathering data from KTL history {cmd}')

    result = sp.Popen(cmd, stdout=sp.PIPE)
    out = result.stdout.readlines()

    # Write data from 14:00 HST for 1 day

    end    = start + dt.timedelta(days=1)
    with open(writeFile, 'w') as f:
        for num,line in enumerate(out):
            line = line.decode('utf-8')
            print(line)
            split = line.split(',')
            if len(line) == 0:
                continue
            check = start if num == 0 else \
              dt.datetime.strptime(split[0], "%Y-%m-%dT%H:%M:%S.%f")
            if start <= check < end:
                f.write(line)

    if log_writer:
        log_writer.info(f'kpfsocal.py complete for {utDate}')
