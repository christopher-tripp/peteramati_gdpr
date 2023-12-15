# Import relevant libraries and packages.
import sys
import subprocess
import mysql.connector
import json
from datetime import datetime


def grab_data(f, cursor):
    """
    Given an output file and an initialized mysql.connector cursor, write the
    relevant personal data for the associated user to the output file.

    Required arguments:
    f       file
    cursor  mysql.connector cursor

    Returns:
    None
    """

    print("Collecting and writing data...")

    # Start of CONTACT INFORMATION
    f.write("CONTACT INFORMATION:\n----------\n")

    # ContactInfo
    role_list = ["student", "TA", "instructor"]
    role = ""
    github_name = ""
    q = ("SELECT firstName, lastName, email, lastLogin, roles, "
         "anon_username, github_username FROM ContactInfo WHERE "
         "email='{0}'".format(user_email))
    cursor.execute(q)
    for (firstName, lastName, email, lastLogin, roles, 
         anon_username, github_username) in cursor:
        role = role_list[roles]
        github_name = github_username
        full_name = firstName + " " + lastName
        lastLogin_time = datetime.utcfromtimestamp(lastLogin)
        if (github_username is not None):
            github_username = github_username.decode()
        if (anon_username is not None):
            anon_username = anon_username.decode()
        if (role == "student"):
            w = ("Name: {0}\nEmail: {1}\n"
                "Time of last login: {2}\nRole: {3}\nAnonymous username: {4}\n"
                "Github username: {5}".format(full_name, email,
                                              lastLogin_time, role, 
                                              anon_username,
                                              github_username))
        else: 
            w = ("Name: {0}\nEmail: {1}\n"
                "Time of last login: {2}\nRole: {3}".format(full_name, email,
                                                            lastLogin_time, 
                                                            role))
        f.write(str(w))


    # Start of ACTIVITY INFORMATION
    f.write("\n\n\nACTIVITY INFORMATION:\n----------\n")
    
    # ContactLink
    # If user is student, get partners who are linked to this user.
    if (role == "student"):
        q = ("SELECT firstName, lastName FROM ContactInfo WHERE contactID in "
             "(SELECT link FROM ContactLink WHERE (cid='{0}' AND "
             "type='1'))".format(user_id))
        cursor.execute(q)
        for (firstName, lastName) in cursor:
            partner_name = firstName + " " + lastName
            w = "Partnered with: {0}\n".format(partner_name)
            f.write(w)
    
    # ActionLog
    q = ("SELECT time, action FROM ActionLog WHERE action "
         "LIKE '%{0}%'".format(user_github))
    cursor.execute(q)
    for (time, action) in cursor:
        w = "Action: {0}\n\tTime: {1}\n".format(action, time)
        f.write(w)


    # Start of GRADE INFORMATION
    f.write("\n\nGRADE INFORMATION:\n----------\n")

    # ContactGrade is for gitless psets.
    if (role == "student"):
        q = ("SELECT pset, notes, hidegrade FROM ContactGrade WHERE "
             "cid='{0}'".format(user_id))
        cursor.execute(q)
        for (pset, notes, hidegrade) in cursor:
            if (hidegrade == 0):
                f.write("Problem set: {0}\n".format(pset))
                notes_data = json.loads(notes.decode())
                for item in notes_data["grades"]:
                    score = notes_data["grades"][item]
                    w = ("\tGrade ({0}): {1}\n".format(item, str(score)))
                    f.write(w)
    else:
        q = ("SELECT ContactGrade.cid, ContactGrade.pset, ContactGrade.notes, "
             "ContactInfo.email, ContactInfo.contactId FROM ContactGrade, "
             "ContactInfo WHERE ContactGrade.gradercid='{0}' "
             "AND ContactGrade.cid = ContactInfo.contactId".format(user_id))
        cursor.execute(q)
        f.write("Problem sets graded by you:\n\n")
        for (cid, pset, notes, email, cid) in cursor:
            f.write("Student: {0}\n".format(email))
            f.write("Problem set: {0}\n".format(pset))
            notes_data = json.loads(notes.decode())
            for item in notes_data["grades"]:
                score = notes_data["grades"][item]
                w = ("\tGrade ({0}): {1}\n".format(item, str(score)))
                f.write(w)
            f.write("\n")

    # ContactGradeHistory
    if (role == "student"):
        q = ("SELECT pset, antiupdate FROM ContactGradeHistory WHERE "
             "cid='{0}'".format(user_id))
        cursor.execute(q)
        for (pset, notes) in cursor:
            f.write("Problem set: {0}\n".format(pset))
            notes_data = json.loads(notes.decode())
            for item in notes_data["grades"]:
                score = notes_data["grades"][item]
                w = ("\tGrade ({0}): {1}\n".format(item, str(score)))
                f.write(w)
    else:
        q = ("SELECT pset, antiupdate FROM ContactGradeHistory "
             "WHERE updateby='{0}'".format(user_id))
        cursor.execute(q)
        for (pset, notes) in cursor:
            f.write("Problem set: {0}\n".format(pset))
            notes_data = json.loads(notes.decode())
            for item in notes_data["grades"]:
                score = notes_data["grades"][item]
                w = ("\tGrade ({0}): {1}\n".format(item, str(score)))
                f.write(w)

    # Start of student REPOSITORY INFORMATION
    if (role == "student"):
        f.write("\n\nREPOSITORY INFORMATION:\n----------\n")

        # Repository table
        q = ("SELECT url, working FROM Repository WHERE "
            "url LIKE '{0}%'".format(user_repo_url))
        cursor.execute(q)
        for (url, working) in cursor:
            w = ("Repository URL: {0}\n\tSnap commit at: "
                 "{1}".format(url.decode(), 
                             datetime.utcfromtimestamp(working)))
            f.write(w)

        # Get branches which are linked to our user through a row 
        # in ContactLink.
        q = ("SELECT branch FROM Branch WHERE branchid IN "
            "(SELECT link FROM ContactLink WHERE "
            "cid={0})".format(user_id))
        cursor.execute(q)
        for branch in cursor:
            f.write("Repository branch: {0}".format(branch))

    # MailLog
    f.write("\n\nEMAIL INFORMATION:\n----------\n")
    q = ("SELECT recipients, cc, subject, emailBody FROM MailLog WHERE "
         "recipients LIKE '%{0}%' "
         "OR cc LIKE '%{0}%'".format(user_email))
    cursor.execute(q)
    for (recipients, cc, subject, emailBody) in cursor:
        w = ("Email subject: {0}\nEmail body: {1}\n\n".format(
                                            subject, emailBody))
        f.write(w)

    print("Finished writing data.")


def delete_data(cursor):
    """
    Given an initialized mysql.connector cursor, delete the corresponding
    user data.

    Required arguments:
    cursor      mysql.connector cursor

    Returns:
    None
    """

    print("Starting deletion process.")

    # ActionLog
    # If user is a student, delete mentions of their github.
    if (userrole == 0):
        q = ("DELETE FROM ActionLog "
             "WHERE action LIKE '%{0}%'".format(user_github))
        cursor.execute(q)
        print("Deleted relevant ActionLog entries.")

    # ContactImage
    q = ("DELETE FROM ContactImage WHERE contactId='{0}'".format(user_id))
    cursor.execute(q)
    print("Deleted relevant ContactImage entries.")

    # Repository and Branch
    if (userrole == 0):
        q = ("DELETE FROM Repository WHERE "
            "url LIKE '{0}%'".format(user_repo_url))
        cursor.execute(q)
        print("Deleted relevant Repository entries.")
        # Get branches which are linked to our user through a row 
        # in ContactLink.
        q = ("DELETE FROM Branch WHERE branchid IN "
            "(SELECT link FROM ContactLink WHERE "
            "cid={0})".format(user_id))
        cursor.execute(q)
        print("Deleted relevant Branch entries.")

    # ContactLink
    q = ("DELETE FROM ContactLink WHERE cid='{0}'".format(user_id))
    cursor.execute(q)
    print("Deleted relevant ContactLink entries.")

    # ContactInfo
    q = ("DELETE FROM ContactInfo WHERE email='{0}'".format(user_email))
    cursor.execute(q)
    print("Deleted relevant ContactInfo entries.")

    print("Finished deletion process.")


if __name__ == "__main__":

    args = sys.argv[1:]

    # If the wrong number of arguments are given, print
    # a help message about the correct format.
    if len(args) < 2:
        print("Arguments: [get_data/delete_data] "
              "[useremail] [outputfilename]")
        sys.exit(0)

    # Establish database connection.
    cmd = """cat peteramati/conf/options.php | grep 'Opt\["dbName"\] ='"""
    result = subprocess.run(cmd, capture_output=True, 
                            shell=True, text=True).stdout
    db_name = (result[len('$Opt["dbname"] = "'):])[0:-3]

    cmd = """cat peteramati/conf/options.php | grep 'Opt\["dbUser"\] ='"""
    result = subprocess.run(cmd, capture_output=True, 
                            shell=True, text=True).stdout
    db_username = (result[len('$Opt["dbUser"] = "'):])[0:-3]
    
    cmd = """cat peteramati/conf/options.php | grep 'Opt\["dbPassword"\] ='"""
    result = subprocess.run(cmd, capture_output=True, 
                            shell=True, text=True).stdout
    db_pw = (result[len('$Opt["dbPassword"] = "'):])[0:-3]

    print("Connecting to database '{0}'...".format(db_name))

    cnx = mysql.connector.connect(user=db_username, 
                                  password=db_pw,
                                  database=db_name)

    print("Database connection successful.")
    

    # Define cursor object.
    cursor = cnx.cursor()

    # Define the user whose info we're going to grab/delete.
    user_email = args[1]

    # Grab the contactId and github_username fields for the user, 
    # since these are needed to identify the user in various tables.
    get_userid_query = ("SELECT firstName, lastName, contactId, "
                        "github_username, roles FROM ContactInfo WHERE "
                        "email='{0}'".format(user_email))
    cursor.execute(get_userid_query)

    # Print some information about what we just executed.
    #print("Executed: {0}".format(cursor.statement))
    #print("Rows changed: {0}".format(cursor.rowcount))
    #print("Warnings generated: {0}\n".format(cursor.fetchwarnings()))

    # Store the returned data in some local variables.
    firstName, lastName, user_id, user_github, userrole = cursor.fetchone()
    if (userrole == 0 and user_github is not None):
        user_github = user_github.decode()  # Get string of bytearray.

    # Construct base of url for this user's remote repo.
    if (userrole == 0 and user_github is not None):
        user_repo_url = ("https://github.com/" + 
                        user_github + "/")
    elif (userrole != 0):
        user_repo_url = "not applicable for TA/instructor"
    else:
        user_repo_url = "No github account found."

    # If our query returned more than one row, raise an exception.
    if cursor.fetchone() is not None:
        raise ValueError("More than one row was found in the "
                        "ContactInfo table with the email "
                        "address {0}".format(user_email))

    # Print information about what we found.
    full_name = firstName + " " + lastName
    print("User with email {0} found.\n\tName: {3}\n\t"
          "Contact ID: {1}\n"
          "\tGithub remote: {2}".format(user_email, user_id, 
                                        user_repo_url, full_name))
    
    # Determine whether we're going to be grabbing data or
    # deleting data, and proceed accordingly.
    if args[0] == "get_data":
        # Check that we have the right number of arguments.
        if (len(args) != 3):
            print("Insufficient number of arguments given.  You might have "
                  "forgotten to provide the outputfile name in the third "
                  "argument.")
        else:
            # Create an output file.
            f = open(args[2], "w")

            # Grab the data.
            grab_data(f, cursor)

            # Close our output file.
            f.close()
            print("File", args[2], "closed.")

    elif args[0] == "delete_data":

        # First check with the user on the CLI to make sure
        # that they actually want to delete.
        print("Are you sure you want to delete data? [Y/n]")
        delete_input = input()
        if (delete_input == "Y"):
            delete_data(cursor)
            # Make sure any changes are committed to the database.
            cnx.commit()
        else:
            print("Input not understood.  Aborting.")

    else:
        print("Incorrect command formatting. Aborting.")

    # Close the cursor and connection.
    cursor.close()
    cnx.close()
    print("Database connection closed.")

