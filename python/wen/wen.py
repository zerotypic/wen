#!/usr/bin/python3
#
# wen : Command-line history management
#

import sys
import os
import datetime
import sqlite3

DB_DEFAULT_PATH = "~/.wendb"
DB_APPLICATION_ID = 0x6e6577    # 'wen'
DB_VERSION = 7
DB_CREATE_STMTS = (
    "PRAGMA application_id = {};".format(DB_APPLICATION_ID),
    "PRAGMA user_version = {};".format(DB_VERSION),
    "CREATE TABLE entries (entrytype INTEGER, ts DATETIME, pid INTEGER, cmdline TEXT);",
    "CREATE INDEX entries_ts_idx ON entries (ts);",
    "CREATE INDEX entries_pid_ts_idx ON entries (pid, ts);"
)

IGNORE_LIST = ("pwd", "ls")

class Exn(Exception): pass
class DBExn(Exn): pass

def LOG(m):
    print("{}: {}".format(sys.argv[0], m))
#enddef

class WenDB(object):

    ENTRYTYPE_HISTORY = 0
    ENTRYTYPE_SESSION_START = 1
    ENTRYTYPE_SESSION_STOP = 2
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.db = sqlite3.connect(db_path)
    #enddef

    def init_db(self):
        for stmt in DB_CREATE_STMTS:
            self.db.execute(stmt)
        #endfor
        self.db.commit()
    #enddef
   
    def check_db(self):
        db_app_id = self.db.execute("PRAGMA application_id").fetchall()[0][0]
        db_version = self.db.execute("PRAGMA user_version").fetchall()[0][0]

        if db_app_id != DB_APPLICATION_ID:
            raise DBExn("Database does not have correct application ID.")
        elif db_version != DB_VERSION:
            raise DBExn("Database does not have correct version.")
        else:
            return True
        #endif
    #enddef

    def insert_history(self, ts, pid, cmdline):
        self.db.execute("INSERT INTO entries (entrytype, ts, pid, cmdline) VALUES (?,datetime(?, 'unixepoch'),?,?)",
                        (self.ENTRYTYPE_HISTORY, ts, pid, cmdline))
        self.db.commit()
    #enddef

    def insert_session_start(self, ts, pid, cmdline):
        self.db.execute("INSERT INTO entries (entrytype, ts, pid, cmdline) VALUES (?,datetime(?, 'unixepoch'),?,?)",
                        (self.ENTRYTYPE_SESSION_START, ts, pid, cmdline))
        self.db.commit()
    #enddef
    
    def insert_session_stop(self, ts, pid, cmdline):
        self.db.execute("INSERT INTO entries (entrytype, ts, pid, cmdline) VALUES (?,datetime(?, 'unixepoch'),?,?)",
                        (self.ENTRYTYPE_SESSION_STOP, ts, pid, cmdline))
        self.db.commit()
    #enddef

    def get_last_command(self, pid):
        cur = self.db.execute("SELECT cmdline FROM entries WHERE entrytype=? AND pid=? ORDER BY ts DESC LIMIT 1",
                              (self.ENTRYTYPE_HISTORY, pid))
        row = cur.fetchone()
        return row[0] if row != None else None
    #enddef
    
    def get_sessions(self, limit=None):

        cur = self.db.execute(
            "SELECT entrytype, cast(strftime('%s', ts) as integer), pid, cmdline" + \
            " FROM entries ORDER BY ts DESC LIMIT {:d}".format(limit if limit != None else -1)
        )

        sessions = {}
        def add_to_sessions(pid, history):
            if not pid in sessions: sessions[pid] = []
            history.reverse()
            sessions[pid].append(history)
        #enddef

        acc = {}
        for (entrytype, ts, pid, cmdline) in cur:
            pidstr = pid
            if entrytype == self.ENTRYTYPE_HISTORY:
                if not pid in acc: acc[pid] = []
                acc[pid].append((ts, cmdline))
            elif entrytype == self.ENTRYTYPE_SESSION_START:
                if pid in acc:
                    add_to_sessions(pid, acc[pid])
                    del acc[pid]
                #endif
            elif entrytype == self.ENTRYTYPE_SESSION_STOP:
                if pid in acc:
                    add_to_sessions(pid, acc[pid])
                    del acc[pid]
                #endif
            #endif
        #endfor

        for pid in acc: add_to_sessions(pid, acc[pid])
            
        flattened = {}
        for (pid, histories) in sessions.items():
            for (i, history) in enumerate(histories):
                name = "{:d}_{:d}".format(pid, i) if i > 0 else str(pid)
                flattened[name] = history
            #endfor
        #endfor
       
        return flattened
        
    #enddef
    
    def close(self):
        self.db.close()
    #enddef
    
#endclass

def do_start_session(db, ts, pid, cmdline):
    db.insert_session_start(ts, pid, cmdline)
#enddef

def do_stop_session(db, ts, pid, cmdline):
    db.insert_session_stop(ts, pid, cmdline)
#enddef

def do_append(db, ts, pid, cmdline,
              ignore_dups=True, ignore_space=True):

    # Ignore empty commandlines
    if cmdline == None or cmdline == "": return
    
    if ignore_space and cmdline[0] == " ": return

    # Ignore items in ignorelist
    if cmdline.strip() in IGNORE_LIST: return
    
    if ignore_dups:
        prev = db.get_last_command(pid)
        if prev != None and prev == cmdline: return
    #endif

    db.insert_history(ts, pid, cmdline)
    
#enddef

def do_show(db):

    sessions = db.get_sessions()
    def hist_key(hist):
        if hist == []: return None
        return hist[-1][0]
    #enddef
    # Sort based on latest timestamp of each session
    items = sorted(sessions.items(),
                   key=lambda it: hist_key(it[1]))
    
    for (name, history) in items:
        print("# --- SESSION {} --- #".format(name))
        for (ts, cmdline) in history:
            print("{}\t{}".format(datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d  %H:%M:%S"),
                                  cmdline))
        #endfor
        print("# --- END SESSION {} --- #".format(name))
    #endfor
#enddef

def main():

    if sys.argv[0].endswith("wen-append"):

        # Fast path.
        db_path = os.path.abspath(os.path.expanduser(DB_DEFAULT_PATH))
        fix = True
        pid = os.getppid()
        ts = int(datetime.datetime.now().timestamp())
        cmdline = sys.argv[1]
        cmd = "append"
        ignore_dups = True
        ignore_space = True

    else:

        import argparse
        parser = argparse.ArgumentParser(
            description="Command-line history manager.",
        )

        parser.add_argument("-H", "--history-db",
                            action="store",
                            default=DB_DEFAULT_PATH,
                            help="Path to wen history database file")
        parser.add_argument("-f", "--fix",
                            action="store_true",
                            help="Automatically fix invalid database by replacing it.")

        parser.add_argument("-c", "--cmdline",
                            type=str,
                            help="Commandline to append")
        parser.add_argument("-p", "--pid",
                            type=int,
                            help="PID associated with commandline.")
        parser.add_argument("-t", "--ts",
                            type=int,
                            help="Unix timestamp associated with commandline.")
        parser.add_argument("-d", "--no-ignore-dups",
                            action="store_true",
                            help="Include duplicate commands.")
        parser.add_argument("-s", "--no-ignore-space",
                            action="store_true",
                            help="Inclue commands that start with a space.")
        parser.add_argument("cmd",
                            choices=("start-session", "stop-session", "append", "show"),
                            nargs="?",
                            const="show",
                            help="Command to carry out.")

        args = parser.parse_args()

        db_path = os.path.abspath(os.path.expanduser(args.history_db))
        fix = args.fix
        pid = os.getppid() if args.pid == None else args.pid
        ts = int(datetime.datetime.now().timestamp()) if args.ts == None else args.ts
        cmdline = args.cmdline
        cmd = args.cmd
        ignore_dups = not args.no_ignore_dups
        ignore_space = not args.no_ignore_space

    #endif
    
    if os.path.exists(db_path):
        db = WenDB(db_path)
        try:
            db.check_db()
        except DBExn as e:
            LOG("Database Error: {}".format(str(e)))
            db.close()
            if fix:
                backup_path = db_path + datetime.datetime.now().strftime("_%Y%m%d_%H%M%S")
                LOG("Replacing invalid database file, saving old file as {}".format(backup_path))
                os.rename(db_path, backup_path)
                db = WenDB(db_path)
                db.init_db()
            else:    
                return -1
            #endif
        #endtry
    else:
        db = WenDB(db_path)
        db.init_db()
        LOG("Initialized new database.")        
    #endif



    if cmd == "start-session":
        do_start_session(db, ts, pid, cmdline)
    elif cmd == "stop-session":
        do_stop_session(db, ts, pid, cmdline)
    elif cmd == "append":
        do_append(db, ts, pid, cmdline,
                  ignore_dups = ignore_dups,
                  ignore_space = ignore_space)
    else:
        do_show(db)
    #endif
    
    db.close()
    
#enddef

if __name__ == "__main__": sys.exit(main())
