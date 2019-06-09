Ebakup - a backup maintenance system
====================================

The official site for ebakup is on
[eirikba.org](http://eirikba.org/projects/ebakup).

ebakup was intended as a simple backup system based on my personal
wishes for what the backup system I want to use should provide. It
turns out that this makes it not really a "backup" system at all.

What I want is a system where I can find copies of all the data I have
at any time designated as "precious". This system must store both the
data, any important meta-data as well as the structure of the data.
Once something is stored in the backup storage, it should remain there
until it is explicitly removed again. And it should protect all that
data (and meta-data) from corruptions.

And of course, the system should make all of this as easy as possible.

So for the backups themselves:

- Configuration specifies which parts of the file system are "precious"
- A "snapshot" stores all the "precious" data at that time
  - The content of each file
  - The tree structure of files and directories
  - Any relevant/important meta-data for both files and directories
- All data added to the backup storage remain until explicitly removed
- All information in the backup storage can be checked for corruptions
- All information is stored in multiple copies (so that if any one
  copy is destroyed, it can be restored from another copy).
- Small corruptions in the backup storage does not cause large-scale
  corruptions of the stored data.
- Support for tracking files that should never change

Backups are no good if you can not get data out again:

- Single files can be copied from any historical snapshot
- Subtrees can be copied from any historical snapshot

Other features that could be nice to have (I may add them some day):

- Storing backups in cloud storage (backblaze, amazon, google, microsoft)
- Support for encrypted backups (particularly for cloud storage)

Features I don't particularly care about (and so I'm unlikely to
implement support for it):

- Restoring a full system from a backup to recover from catastrophic
  failure.


Status
------

(As of 2019-06-09)

The backup system makes backups successfully. I'm using it myself for
that purpose. That's not necessarily a very strong endorsement, given
that the alternative is no backup at all. I have also successfully
restored files, although that is still a highly manual process.

So what works?

- Creating backup snapshots
  - of a single "root" directory
  - with include/exclude paths
  - checksums everywhere
- All database data is verified every time it is used
- Verify operation checks all content data
- Syncing storages (mostly)
- Creating a "shadow copy" of a snapshot (using hardlinks)
- Any problem leads to ebakup exiting with an error message, not
  silently introducing errors into the backups

What's missing?

- Copy-out files and trees from a snapshot
- "Verify" operation does not check the database files
- Repair (replace corrupt data with good data from other source)
- Edge case handling for syncing storages
  - (I believe sync works correctly in most cases, but I know I didn't
    meticulously construct that code for being always correct.)
- Files that change during the backup operation
- Nice UI

The current python code is painfully slow at decoding the database
files. I need to do something about that. I'm considering rewriting it
in C++. It is also using a lot of memory, which I also think a rewrite
to C++ could help with.

As I said in February 2016, I have learned a few things since I
started this project.

I have learned a lot about what ebakup is, or should be. Which means I
now want to make a lot of changes to the system. Particularly to the
various terms I have used, which just aren't right.

I have learned a lot about doing a complete software project properly.
Unfortunately, in many cases by getting things wrong. I think this is
the first private project I have ever done that has gone past the
"half-finished hack" stage. So there's a need for a lot of cleanup in
the codebase. Particularly to the test code.


Requirements background
-----------------------

It seems to me that most backup systems are based on the idea of
recovering from catastrophic failure. That is, they primarily offer
the promise that if your computer completely self-destructs one day,
you can set up a new computer and make the disk a perfect copy of what
you had on the old computer the last time you made a backup.

This is not what I want. If my computer suddenly vanishes into a
parallel dimension one day, I will buy a new computer, install the
system from scratch and copy any files I care about from some
alternative source. Making sure such an alternative source exists is
primarily what ebakup is about. That, and being able to restore
individual files that somehow get lost or corrupt.

A secondary feature that I think is nice is to have long-term history.
Having old versions of all files available so I can see what they were
like at the time. This isn't in itself that useful to me, since I keep
most of the things I make in git. But I feel it makes sense. (And
there are some things that, for some reason or other, is not in git.)

It does, however, lead to a really useful feature: Files that *only*
exist in the backup. Once a file is added to the backup, it remains
available indefinitely (at least unless it is explicitly purged). Thus
it is reasonable to delete files from the "real" system and rely on
being able to recover it from the backups. Even years or decades in
the future.

(As an aside: Now that I have actually constructed a working backup
system, I should really have a look at the existing ones again. Just
in case one of them actually provides what I want. Then again, I
recall that investigating backup systems was a real pain.)


Basic design
------------

The backup system stores snapshots of the source files. The content of
each file is stored in the "content storage", without duplicates. All
other information (the files' paths and any metadata) are stored in
the "database". Each snapshot has this information (for robustness
reasons) stored in its own, standalone, immutable file.

In addition, the backup system can create a "shadow tree" that
recreates the backup's tree structure using hard links into the
content storage. This tree used to be constructed automatically
whenever a new snapshot was made. But I think that was a bad UI
choice, so I removed that feature. There is now a "shadowcopy" command
that will create this tree when needed instead.


Robustness
----------

What good is a backup if I can't trust that it is correct? Since
there's no good way to ensure the backup won't get corrupted, I have
added the ability to verify the correctness of pretty much everything
in the backup.

Every file in the database stores a checksum for every "block" in the
file (currently sha-256 in every 4096-byte block). This makes it
exceptionally unlikely that the data in the database will have random
errors that can't be detected. And when such an error is found, the
"untrusted" part of the backup will be quite small.

These block checksums are checked every time a block is read, so no
action will ever be taken on corrupt data.

Similarly, every item added to the content store has its checksum
(also currently sha-256) stored. Thus undetectable corruptions to the
content of files are also extremely unlikely. There's currently no
verification of these checksums. Personally, I sometimes run a full
recursive diff on my content storages as a simple form of corruption
detection.

So what happens when things do get corrupt? If ebakup's support for
multiple copies of backups have been used, hopefully there should be
at least one copy somewhere that is not corrupt. Which can then be
used to replace the corrupt copies. (Though currently there's no UI
for doing that. I need to add that too.)
