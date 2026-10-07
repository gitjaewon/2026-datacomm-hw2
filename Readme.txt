HW#2 Thread Pool 기반 좌석 예매 서버


1. 조원

조: G4

이름   / 학번     / 역할
김재원 / 20213086 / Server·Listener, RequestQueue·Worker, 원격 배포, 영상
윤도훈 / 20223122 / Client·ClientMain, 로그, Verify
김지현 / 20223097 / SeatManager, Notifier


2. 프로젝트 개요

본 프로젝트에서는 여러 Client가 동시에 접속할 수 있는 좌석 예매 Server를 구현하였다.
통신은 TCP Socket을 사용하며 Server는 100개의 좌석을 관리한다.

구현 언어는 Java이며, Server는 AWS EC2 t3.micro (Ubuntu 26.04 LTS, OpenJDK 21)에서
실행하고 Client 30개는 로컬 PC에서 실행하였다.

Client가 보낼 수 있는 요청은 단일 좌석 예약(RESERVE), 다중 좌석 예약(RESERVE_MULTI),
예약 취소(CANCEL)이다.

Server에서는 Listener가 Client의 요청을 받아 Request Queue에 넣고,
미리 생성해 둔 10개의 Worker Thread가 이 요청을 꺼내 처리한다.
요청마다 Thread를 새로 만들지 않고 같은 Worker를 계속 재사용한다.

여러 Worker가 동시에 같은 좌석에 접근할 수 있기 때문에 좌석마다 Lock을 하나씩 두었다.
한 좌석의 owner와 Waitlist는 반드시 해당 좌석의 Lock을 획득한 상태에서만 읽거나 변경한다.

RESERVE_MULTI는 여러 개의 Lock이 필요하므로 요청받은 좌석 번호를 먼저 정렬하고
작은 번호부터 Lock을 획득한다. 요청한 좌석 중 하나라도 EMPTY 상태가 아니면
전체 요청을 실패시키고 좌석 상태는 변경하지 않는다.

RESERVE로 요청한 좌석을 다른 Client가 가지고 있는 경우에는 바로 실패시키지 않고
해당 좌석의 Waitlist에 등록한다. 이후 현재 owner가 좌석을 취소하면 FIFO 순서에 따라
가장 먼저 기다린 Client에게 좌석을 넘기고 NOTIFY를 보낸다.

최종 테스트는 원격 Server 1대와 로컬 Client 30개로 진행하였다.
각 Client가 5,000개의 요청을 보내므로 전체 요청은 150,000건이다.

테스트가 끝난 뒤에는 Server 로그와 Client 로그를 비교하여 이중예약이 없었는지,
좌석의 최종 owner가 일치하는지 확인하였다. 또한 Waitlist 등록 수가 NOTIFY 수와
종료 시 미해결 대기 수의 합과 일치하는지도 확인하였다.

Deadlock은 Request Queue에 요청이 남아 있는데 일정 시간 동안 처리가 진행되지 않는
경우가 있었는지로 확인하였다.

추가로 처리량, 평균 응답 시간, Queue 최대 길이, Waitlist 평균 대기시간,
Lock 경합 횟수를 측정하였다.


3. 배포 정보

Server: AWS EC2 t3.micro (eu-north-1), Ubuntu 26.04 LTS, OpenJDK 21
        포트 5000, IP ***.***.***.***

Client: 로컬 PC (Windows 11, Java 21)에서 30개 실행

로그 시간대: Server/Client 모두 KST
             EC2의 시스템 시간대는 UTC이지만 Log.java에서 Asia/Seoul로 지정하여
             Server 로그도 KST로 기록하였다.

Server가 스톡홀름 리전에 위치하여 평균 응답 시간에는
약 250~300ms의 네트워크 왕복 지연이 포함된다.


4. 구성

Server.java        서버 시작/종료 흐름, 통계, 종료 보고서
Listener.java      접속 수락, Selector로 소켓 30개 감시, 메시지 파싱 후 큐에 넣기,
                   POOL 로그, Deadlock 감시
Conn.java          연결 1개 (수신 버퍼, 소켓별 송신 Lock)
RequestQueue.java  Listener -> Worker 큐 (Lock + Condition)
Worker.java        Worker 10개. 큐에서 꺼내서 좌석 판정 후 응답
SeatManager.java   좌석 100개, RESERVE / RESERVE_MULTI / CANCEL 판정
Notifier.java      Notify Queue에서 Condition으로 대기하다 깨어나 대기자에게 NOTIFY 전송
Log.java           로그 출력
ClientMain.java    Client 30개를 한 프로세스 안의 스레드로 실행
Client.java        Client 1개 (송신 스레드 + 수신 스레드)
Verify.java        실행 후 로그로 정합성 확인
Arguments.java     공통 실행 인자 검사 (필수 주소, 포트 범위, 양수 요청 수)

서버 스레드: Listener 1(main), Worker 10, Notifier 1. 그 외 프로그램 스레드는 없음.

POOL 로그와 Deadlock 감시는 별도 스레드 없이 Listener의 select(500ms) 타임아웃마다
경과 시간을 확인하여 처리함. POOL 로그는 5초마다 출력함.

메시지 형식 (한 줄 = 한 메시지, \n으로 구분)

Client -> Server:
HELLO <id>
RESERVE <reqId> <seat>
RESERVE_MULTI <reqId> <s1,s2,..>
CANCEL <reqId> <seat>

Server -> Client:
RESP <reqId> SUCCESS
RESP <reqId> FAIL <사유>
RESP <reqId> WAITLISTED
NOTIFY <reqId> <seat>
BYE

reqId는 Client가 1부터 붙이고 응답과 NOTIFY에 그대로 들어감.
응답 순서가 섞여도 reqId로 맞춤.

Client 번호는 1~30, reqId는 1~requests이며,
Server는 실행 중 이미 처리한 reqId의 재사용이나 중복 요청을 무시함.

Client는 NOTIFY를 단일 RESERVE의 reqId와 좌석이 모두 일치할 때 한 번만 반영함.
NOTIFY가 WAITLISTED보다 먼저 와도 Client는 좌석 배정을 즉시 반영하고,
나중에 오는 첫 응답과 별도로 집계함.

TCP는 메시지 경계가 없으므로 Server는 받은 데이터를 Conn의 수신 버퍼에 쌓고
\n마다 한 줄씩 처리하며, Client는 수신 스레드에서 한 줄씩 읽어 처리함.


5. 컴파일 및 실행 (JDK 17 이상)

javac --release 17 -encoding UTF-8 -d out src/*.java

java -cp out Server --host 0.0.0.0 --port 5000
java -cp out ClientMain --host <서버 IP> --port 5000
java -cp out Verify --log-dir logs

Verify 실행 전 Server.txt와 Client1~30.txt를 logs 폴더에 둔다.

인자

--host
  Server/Client 필수.
  Server는 바인딩 IP, Client는 서버 주소.
  하드코딩하지 않으며 값이 없으면 실행하지 않음.

--port
  Server/Client 필수.
  1~65535 범위.

--requests
  Client당 요청 수. 기본값 5000.
  개발 테스트에서 요청 수를 줄일 경우 Server, ClientMain, Verify에
  같은 값을 사용해야 함.
  Server는 30 x requests 건에 대해 첫 응답을 모두 보내면 종료함.

--log-dir
  로그 폴더. 기본값 logs.
  Server, ClientMain, Verify에서 사용함.

--min-interval-ms
  Client 개발용 최소 전송 간격. 기본값 200ms.

--max-interval-ms
  Client 개발용 최대 전송 간격. 기본값 1000ms.

host는 빈 값을 허용하지 않고 port는 1~65535,
requests와 전송 간격은 양수여야 한다.
잘못된 인자는 프로그램 시작 전에 거부한다.

정식 실행에서는 간격 인자를 생략하여 0.2~1.0초 간격을 사용한다.

다음 값은 코드에 고정하였다.

Client 수: 30
Request Queue 크기: 1000
POOL 로그 주기: 5초
Deadlock 감시 기준: 30초
인기 좌석: 1~10번, 70%

최종 실행 시간은 약 51분이었다.


6. Thread Pool

시작할 때 Worker Thread 10개를 생성하고 서버가 종료될 때까지 계속 재사용한다.
요청이나 Client 연결마다 새로운 Thread를 생성하지 않는다.

Request Queue는 ArrayDeque + ReentrantLock + Condition으로 직접 구현하였다.
notEmpty, notFull 두 개의 Condition을 사용한다.

- Queue가 비어 있으면 Worker는 notEmpty.await()로 대기한다.
- Listener가 요청을 Queue에 넣으면 notEmpty.signal()로 대기 중인 Worker를 깨운다.
- Queue의 최대 크기는 1000으로 설정하였다.
- Queue가 가득 차면 Listener는 notFull에서 대기하며 요청을 버리지 않는다.
- Queue가 가득 찬 상태에서는 Listener가 1초마다 깨어나
  POOL 로그와 Deadlock 감시가 필요한지 확인한다.

이 구조를 사용하면 요청마다 Thread를 새로 만드는 비용이 없고,
부하가 증가해도 Worker 수가 10개로 유지된다.
또한 Queue가 가득 찬 경우에도 요청을 버리지 않고 공간이 생길 때까지 기다리도록 하였다.

단점은 인기 좌석에 요청이 몰릴 경우 여러 Worker가 같은 좌석의 Lock을 기다리면서
다른 요청의 처리도 늦어질 수 있다는 점이다.

또한 Queue가 가득 찬 동안에는 Listener가 notFull에서 대기하므로
다른 Socket의 데이터를 바로 읽지 못할 수 있다.
최종 실험에서는 Request Queue의 최대 길이가 충분히 작아 이 상황은 발생하지 않았다.


7. 동시성 제어

좌석마다 ReentrantLock 1개를 사용한다. 총 100개의 좌석 Lock이 있으며,
owner와 waitlist는 해당 좌석의 Lock 안에서만 읽거나 변경한다.

Critical Section은 좌석 상태를 확인하고 변경하는 부분이다.

RESERVE
  비었는지 확인 -> 배정
  이미 내가 가지고 있거나 대기 중이면 FAIL
  다른 Client가 가지고 있으면 Waitlist 등록

RESERVE_MULTI
  필요한 Lock을 모두 잡은 뒤 전체 좌석이 비었는지 확인
  전부 비어 있으면 전체 배정
  하나라도 비어 있지 않으면 아무것도 변경하지 않고 FAIL

CANCEL
  owner 확인 -> 해제
  대기자가 있으면 Waitlist 맨 앞 Client에게 바로 넘김

좌석이 비어 있는지 확인하는 과정과 owner를 변경하는 과정은 같은 Lock 안에서 처리한다.
따라서 두 Worker가 동시에 같은 좌석을 EMPTY라고 판단하고 둘 다 배정하는 상황을 막는다.

좌석 범위와 개수 검사는 Lock을 잡기 전에 처리한다.
응답 전송과 로그 기록은 좌석 Lock을 푼 뒤 처리하여 Lock을 잡고 있는 시간을 줄였다.

Lock 해제는 finally에서 처리하여 중간에 실패하거나 예외가 발생해도 항상 해제되도록 하였다.

그 외에 Request Queue, Notify Queue, 소켓 송신, 로그 파일,
Client 내부 상태에도 각각 필요한 Lock을 사용한다.
좌석 Lock을 잡은 상태에서 이러한 Lock을 추가로 잡지 않도록 구성하였다.

Lock 경합 횟수는 Worker가 좌석 Lock을 tryLock()으로 먼저 획득하려 했으나
실패하여 기다리게 된 횟수로 측정한다.


8. Lock Ordering (Deadlock 회피)

RESERVE_MULTI 처리 순서

1) 좌석 수가 2~4개인지, 좌석 번호가 1~100 범위인지 검사
2) 좌석 번호를 오름차순으로 정렬하고 중복 검사
   예: [5,3] -> [3,5]
3) 정렬된 순서대로 Lock 획득
4) 모든 좌석이 EMPTY이면 전체 배정
   하나라도 EMPTY가 아니면 아무 좌석도 변경하지 않고 FAIL
5) finally에서 획득한 Lock을 모두 해제

모든 Worker가 항상 작은 좌석 번호의 Lock부터 획득하기 때문에
서로 상대방이 가진 Lock을 기다리는 순환 대기가 생기지 않는다.

Lock을 2개 이상 동시에 잡는 경우는 RESERVE_MULTI뿐이며,
나머지 요청에서는 한 번에 하나의 좌석 Lock만 사용한다.

Deadlock은 Request Queue에 처리할 요청이 남아 있는데
30초 동안 처리 완료 수가 증가하지 않는 경우 1회로 집계한다.
이 검사는 감시용이며 Deadlock을 타임아웃이나 강제 종료로 해결하지 않는다.

장점은 좌석 번호를 정렬하는 방식만으로 Lock 획득 순서를 통일할 수 있다는 점이다.

단점은 필요한 Lock을 미리 알아야 하며,
앞쪽 좌석의 Lock을 잡은 상태에서 다음 Lock을 기다리는 동안
다른 요청도 해당 좌석에서 기다릴 수 있다는 점이다.


9. Waitlist / Condition Variable / Notifier

좌석마다 ArrayDeque 기반 FIFO Waitlist를 둔다.

단일 RESERVE에서 다른 Client가 이미 좌석을 보유하고 있으면
해당 Client를 Waitlist의 맨 뒤에 넣고 WAITLISTED를 바로 응답한다.

Worker는 좌석이 비기를 기다리지 않고 다음 요청을 처리한다.

CANCEL이 성공하면 좌석 Lock 안에서 Waitlist의 맨 앞 항목을 pollFirst()로 꺼내
해당 Client에게 좌석을 바로 배정한다.

따라서 좌석이 잠시 EMPTY가 되는 구간이 없으며
다른 Client가 중간에 좌석을 가져갈 수 없다.

Worker가 좌석 Lock 안에서 pollFirst()로 대기자를 꺼내 바로 배정하므로
Waitlist에 등록된 순서대로 좌석이 배정된다.
NOTIFY 도착 순서가 달라지더라도 실제 좌석 배정 순서는 바뀌지 않는다.

좌석 배정이 끝나면 Worker는 좌석 Lock을 풀고
Notify Queue에 통지 작업을 넣은 뒤 signal한다.

Notifier는 Notify Queue에 작업이 없으면 hasWork.await()로 대기하고,
작업이 들어오면 깨어나 Client에게 NOTIFY를 전송한 뒤 Waitlist 대기시간을 기록한다.

서버 종료 시에는 남아 있는 Notify 작업을 모두 처리한 뒤 Notifier를 종료한다.

Worker의 Request Queue 대기와 Notifier의 Notify Queue 대기 모두
Condition Variable을 사용하며 sleep 반복 확인 방식은 사용하지 않는다.


10. 테스트 및 정합성 확인

테스트

- 요청 좌석이 1~10번에 몰리도록 하여 실제 약 63%의 요청이 인기 좌석에 집중되게 하였다.
- 개발 중에는 코드를 임시로 수정하여 인기 좌석을 1~5번으로 제한하고,
  요청 간격을 1~20ms로 줄여 더 강한 경합 상황에서도 여러 번 테스트하였다.
- 모든 테스트에서 이중예약은 0건이었다.
- Client는 RESERVE_MULTI 좌석을 정렬하지 않은 상태로 전송하고,
  Server가 실제 Lock을 항상 오름차순으로 획득하는지 LOCK 로그로 확인하였다.
- Deadlock은 발생하지 않았다.
- 실행 중 jstack을 이용해 프로그램에서 생성한 스레드가
  Listener 1개, Worker 10개, Notifier 1개인지 확인하였다.

정합성 확인

Verify.java로 Server.txt와 Client1~30.txt를 비교한다.
결과는 logs/VerifyResult.txt에 저장한다.

확인 항목

1) 이중예약 0건
2) 배정 수 - 해제 수 = 종료 시 예약 좌석 수
3) Server의 최종 좌석 상태 = Client 30개의 최종 보유 좌석
4) WAITLISTED 수 = NOTIFY 수신 수 + 종료 시 미해결 대기 수
5) 모든 Client가 5,000건을 전송하고 첫 응답을 모두 받은 뒤 BYE를 수신했는지 확인
6) Server 처리 수와 Client 응답 집계가 일치하는지 확인
7) 응답/통지 전송 실패와 Server 처리 예외가 0건인지 확인
8) Deadlock 0건 및 Server 정상 종료 확인

최종 좌석 목록이나 필수 지표가 누락된 로그는 FAIL로 처리하며
Verify의 종료 코드는 1로 설정하였다.

Server 로그의 자체 검사 결과는 참고용이며,
Client 로그와 대조한 최종 PASS 여부는 Verify에서 판정한다.

집계 기준

좌석이 Client에게 배정될 때마다 배정 수 +1
CANCEL 성공 시 해제 수 +1
Waitlist 대기자에게 좌석을 넘길 경우 해제 1 + 배정 1로 집계


실측 결과 (원격 구성, 30 x 5,000건)

처리량                       ______ req/s
Request Queue 최대 길이       ______ 건
이중예약                      ______ 건
Deadlock                      ______ 건
Waitlist 평균 대기시간        ______ sec
Lock 경합                     ______ 건
최종 좌석 정합성              ______

총 요청                       150,000건
SUCCESS                       ______ 건 (______%)
FAIL                          ______ 건 (______%)
WAITLISTED                    ______ 건 (______%)
NOTIFY / 미해결               ______ 건 / ______ 건
평균 응답 시간                ______ ms


참고: 원격 리허설 (30 x 100건)

처리량 47.8 req/s
Request Queue 최대 길이 12
이중예약 0
Deadlock 0
Waitlist 평균 대기시간 13.8 sec
Lock 경합 0
최종 좌석 정합성 PASS
SUCCESS 36.3%
FAIL 45.4%
WAITLISTED 18.2%
평균 응답 시간 301ms

실제 요청 속도인 초당 약 50건에서는 Worker가 동시에 같은 좌석 Lock을
획득하려는 경우가 많지 않아 Lock 경합 수가 작게 측정되었다.


11. 직접 정한 것들

- Request Queue 크기는 1000으로 설정하였다.
  Queue가 가득 차면 Listener가 기다리며 요청을 버리지 않는다.
  요청을 버릴 경우 전체 150,000건의 요청/응답 수가 맞지 않을 수 있기 때문이다.

- 요청 비율은 좌석이 없을 때 RESERVE 60%, RESERVE_MULTI 40%,
  좌석을 보유하고 있을 때 RESERVE 30%, RESERVE_MULTI 20%, CANCEL 50%로 설정하였다.

- 보유 좌석이 5개 이상이면 CANCEL을 우선하도록 하였다.
  특정 Client에 좌석이 지나치게 많이 쌓이는 것을 줄이기 위해서이다.

- 인기 좌석은 70% 확률로 1~10번을 선택하도록 하였다.
  60%로 설정했을 때 CANCEL 요청의 영향으로 전체 인기 좌석 요청 비율이 약 53%로 나와
  50%에 너무 가까워 70%로 조정하였다.

- RESERVE_MULTI는 2~4개의 좌석을 중복 없이 무작위로 선택하고
  Client에서는 정렬하지 않은 순서로 전송한다.
  실제 Lock Ordering은 Server에서 처리한다.

- Client는 SUCCESS 응답이나 NOTIFY를 받은 좌석만 보유 좌석으로 처리한다.
  응답을 기다리는 요청은 reqId별로 저장한다.

- WAITLISTED 상태의 요청과 첫 응답보다 먼저 도착한 NOTIFY를 따로 관리한다.

- CANCEL이 FAIL이면 기존 보유 상태를 유지한다.

- CANCEL 응답을 기다리고 있는 좌석에는 새로운 요청을 보내지 않는다.
  개발 중 두 응답의 순서가 뒤바뀌면서 Client의 보유 상태가 달라지는 문제를
  Verify에서 발견하여 이 처리를 추가하였다.

- Client는 최초 연결 시 Server가 아직 준비되지 않은 경우
  1초 간격으로 최대 30번 연결을 재시도한다.
  한 번 연결된 이후 실행 중 연결이 끊기면 재접속하지 않고,
  해당 실행은 무효로 보고 다시 실행한다.

- 모든 Client 연결이 끊기면 Server는 종료 절차를 진행한다.

- Server.txt는 약 40MB, Client 로그 전체는 약 26MB이며
  로그 내용을 줄이지 않고 zip으로 압축하여 약 7MB 크기로 제출한다.

- 화면에는 시작, 전체 접속 완료, 경고, 최종 결과만 출력하고
  나머지 상세 내용은 로그 파일에 기록한다.

- 시연 영상은 원격 Server 실행 -> Client 30개 접속 ->
  다중 예약 로그 확인 -> 종료 후 Verify PASS 확인 순서로 구성한다.


12. 기타

- Verify.java로 정합성을 자동 확인한다.
- 형식이 잘못된 메시지가 들어와도 Server가 종료되지 않고 FAIL로 응답한다.
  이미 처리한 중복 reqId는 4장의 처리 방식과 같이 무시한다.
- Worker 처리 중 예외가 발생해도 가능한 경우 Client에게 FAIL 응답을 보낸다.
- Client가 응답을 읽지 않아 Server 송신이 10초 이상 진행되지 않으면 연결을 종료한다.
- 송신 재시도는 Selector의 쓰기 가능 이벤트를 이용하며 sleep polling은 사용하지 않는다.
- 처리량은 실제 첫 응답 전송에 성공한 요청 수만 사용한다.
- 응답 전송 실패는 response_failed로 별도 기록하고 종료 검사에서 FAIL 처리한다.
- 실행 시간 측정은 각 노드의 System.nanoTime()을 사용하고,
  로그의 wall-clock 시각은 KST로 기록한다.


13. 제출물 (G4HW2.zip)

src/
Readme.txt
AllDefinedLogs.txt
logs/Server.txt
logs/Client1.txt ~ Client30.txt
logs/VerifyResult.txt
download.txt