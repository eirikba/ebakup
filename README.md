Ebakup - a backup maintenance system
====================================

The official site for ebakup is on
[eirikba.org](http://eirikba.org/projects/ebakup).

ebakup is a simple backup system based on my personal wishes for what
the backup system I want to use should provide:

- Back up a specific set of files
- Easy to look at any version of each file
- Reliable long-term storage of all history
- Robust verification of non-corruptness of backups
- Multiple copies of the backup
- Support for tracking files that should not change
- Resilience to minor corruptness of backups (a few bit errors should
  not make large parts of the backups broken).

Features I don't particularly care about (and so I'm not bothering to
implement support for it):

- Restoring a system from a backup to recover from catastrophic
  failure.


Status
------

(As of 2015-08-09)

The backup system makes backups successfully. I'm using it myself for
that purpose (though that's not a very strong endorsement, given that
the alternative is no backup at all).

I may still make some more backwards-incompatible changes to the
backup storage. Since I am relying on this stuff now, I will have to
make tools to upgrade (and verify correctness of upgrade) in that
case.

The main missing piece now is verification (make sure all data has
been checked for corruption "recently"). And better UI.

The current python code is painfully slow at decoding the database
files. I need to do something about that.


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
primarily what ebakup is about.

A secondary feature that I think is nice is to have long-term history.
Having old versions of all files available so I can see what they were
like at the time. This isn't in itself that useful to me, since I keep
most of the things I make in git. But I feel it makes sense. (And
there are some things that, for some reason or other, is not in git.)

It does, however, lead to a really useful feature: Files that *only*
exist in the backup. Once a file is added to the backup, it remains
available indefinitely (at least unless it is explicitly purged). Thus
it is reasonable to delete files from the "real" system and rely on
being able to recover it from the backups. Even years in the future.

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
reasons) stored in its own, standalone, immutable file. In addition,
the backup system will by default create a "shadow tree" that
recreates the backup's tree structure using hard links into the
content storage. The shadow tree is only for convenience and can be
trivially reconstructed from the database (and the content storage).


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

Similarly, every item added to the content store has its checksum
(also currently sha-256) stored. Thus undetectable corruptions to the
content of files are also extremely unlikely.
