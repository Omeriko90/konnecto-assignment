# konnecto-assignment

## Prerequisites

- Any node version >= 12.

## Running the server

The entry point is `src/server.ts`, use any of the start npm scripts to run it.
the default server port is 8000.

## Running the client
Run NPM start, the default client port is 4208
To log-in just input ANY valid email (aaa@aaa.aa) and ANY password (abc)

## Assignment Overview

Please checkout from main branch, To submit the assignment either create a private repository and share it with us,
Or send us the entire project in a zip.

* disclaimer
There is no need to go explore random parts of the project, assume the code there is complete and working as intended.
However if you do encounter a bug as part of your work feel free to fix it :).

* Task 1
Implementing the segment "list" route in the "server/src/routes/segments/segment-route-handler.ts" file.
look for "todo TASK 1"
The route receives a call from the "client/src/app/pages/segment/segment-list/segment-list.component.ts" component and
returns a list of "ISegmentMetaData" as a response along with a count of the segments in the db.
Full implementation of the route is expected matching the client side signature.


After this task is done and returns data click on any of the rows, it wil redirect you to
"src/app/pages/segment/segment-data/segment-data.component.ts"
for task 2.

* Task 2
Implementing the segment "gender data" route in the "server/src/routes/segments/segment-route-handler.ts" file
look for "todo TASK 2"
The route receives a call from the "client/src/app/pages/segment/segment-list/segment-data.component.ts" component and
returns a list of "ISegmentGenderData" as a response;
Full implementation of the route is expected matching the client side signature.


If you wish to do additional changes but feel it's too time consuming / outside the scope of the task feel free
to add a TODO comment with a short description of what you'd like to do there.

