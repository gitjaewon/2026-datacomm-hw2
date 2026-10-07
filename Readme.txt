HW#2 Thread Pool 기반 좌석 예매 서버

1. 조원
  조: G__
  이름 / 학번 / 역할
  ______ / __________ / 팀장, 전체 설계, Server·Listener, 원격 배포
  ______ / __________ / SeatManager, Notifier
  ______ / __________ / Client, ClientMain
  ______ / __________ / RequestQueue, Worker, 로그, Verify, 영상


2. 배포 정보
  Server: AWS EC2 t3.micro (eu-north-1), Ubuntu 26.04 LTS, OpenJDK 21, 포트 5000, IP ***.***.***.***
  Client: 로컬 PC (Windows 11, Java 21)에서 30개 실행
  시간대: Server/Client 모두 KST. EC2는 UTC지만 Log.java에서 Asia/Seoul로 고정함
  서버가 스톡홀름이라 응답 시간에 왕복 지연(약 250~300ms)이 포함됨


3. 구성
  Server.java        서버 시작/종료 흐름, 통계, 종료 보고서
  Listener.java      접속 수락, Selector로 소켓 30개 감시, 메시지 파싱 후 큐에 넣기, POOL 로그, Deadlock 감시
  Conn.java          연결 1개 (수신 버퍼, 소켓별 송신 Lock)
  RequestQueue.java  Listener -> Worker 큐 (Lock + Condition)
  Worker.java        Worker 10개. 큐에서 꺼내서 좌석 판정 후 응답
  SeatManager.java   좌석 100개, RESERVE / RESERVE_MULTI / CANCEL 판정
  Notifier.java      대기자에게 NOTIFY 전송
  Log.java           로그 출력
  ClientMain.java    Client 30개 실행
  Client.java        Client 1개 (송신 스레드 + 수신 스레드)
  Verify.java        실행 후 로그로 정합성 확인

  서버 스레드: Listener 1(main), Worker 10, Notifier 1. 그 외 스레드 없음.
  POOL 로그와 Deadlock 감시는 Listener의 select(500ms) 타임아웃을 이용함.

  메시지 형식 (한 줄 = 한 메시지, \n으로 구분)
    Client -> Server: HELLO <id> / RESERVE <reqId> <seat> / RESERVE_MULTI <reqId> <s1,s2,..> / CANCEL <reqId> <seat>
    Server -> Client: RESP <reqId> SUCCESS|FAIL <사유>|WAITLISTED / NOTIFY <reqId> <seat> / BYE
  reqId는 Client가 1부터 붙이고 응답과 NOTIFY에 그대로 들어감. 응답 순서가 섞여도 reqId로 맞춤.
  TCP는 메시지 경계가 없어서 서버는 받은 데이터를 버퍼에 쌓고 \n마다 한 줄씩 처리함.


4. 컴파일 및 실행 (JDK 17 이상)
  javac --release 17 -encoding UTF-8 -d out src/*.java
  java -cp out Server --host 0.0.0.0 --port 5000                (원격)
  java -cp out ClientMain --host <서버 IP> --port 5000          (로컬)
  java -cp out Verify --log-dir logs                            (끝난 뒤, Server.txt를 logs에 복사)

  인자 (Server, Client 공통)
    --host      필수. Server는 바인딩 IP, Client는 서버 주소 (하드코딩 안 함, 없으면 실행 안 됨)
    --port      필수
    --requests  Client당 요청 수, 기본 5000. 테스트할 때만 줄이고 양쪽에 같은 값을 줘야 함
                (서버는 30 x requests 건에 첫 응답을 다 보내면 종료함)
    --log-dir   로그 폴더, 기본 logs
  나머지(Client 30개, 간격 0.2~1.0초, 큐 1000, POOL 5초, Deadlock 감시 30초, 인기 좌석 1~10번 70%)는 코드에 고정.
  최종 실행은 약 51분 걸림.


5. Thread Pool
  시작할 때 Worker 10개를 만들어서 끝까지 재사용함. 요청이나 연결마다 스레드를 만들지 않음.
  Request Queue는 ArrayDeque + ReentrantLock + Condition 2개(notEmpty, notFull)로 직접 구현함.
  - 큐가 비면 Worker가 notEmpty.await()로 대기, Listener가 넣을 때 signal
  - 큐 크기 1000, 꽉 차면 Listener가 notFull로 대기 (요청 버리지 않음). 1초마다 깨서 POOL/Deadlock 감시는 계속함
  장점: 스레드 생성 비용이 없고 부하가 늘어도 스레드 수가 고정. 요청 유실 없음.
  단점: 인기 좌석에 요청이 몰리면 Worker들이 같은 Lock을 기다리느라 다른 요청도 늦어짐.
        큐가 꽉 차 있는 동안은 Listener가 다른 소켓을 못 읽음 (실제로는 최대 길이가 작아서 안 생김).


6. 동시성 제어
  좌석마다 ReentrantLock 1개 (100개). owner와 waitlist는 그 좌석 Lock 안에서만 읽고 씀.
  Critical Section은 "좌석 상태 확인 + 변경" 부분만임.
    RESERVE: 비었는지 확인 -> 배정 / 이미 내 것·대기 중이면 FAIL / 남의 것이면 대기열 등록
    RESERVE_MULTI: Lock 다 잡고 전부 비었는지 확인 -> 전부 배정
    CANCEL: owner 확인 -> 해제 -> 대기자 있으면 바로 넘김
  확인과 변경을 같은 Lock 안에서 해야 두 Worker가 동시에 "비었다"고 보고 둘 다 배정하는 일이 없음.
  범위·개수 검사는 Lock 전에 하고, 응답 전송과 로그는 Lock을 푼 뒤에 함 (Lock 잡는 시간 최소화).
  Lock 해제는 finally에서 해서 실패해도 항상 풀림.
  그 외 Lock: 큐, Notify Queue, 소켓 송신(연결마다), 로그 파일, Client 내부 상태. 좌석 Lock을 잡은 채로 이것들을 잡지 않음.
  Lock 경합 횟수: Worker가 좌석 Lock을 tryLock()으로 먼저 잡아보고 실패한 횟수.


7. Lock Ordering (Deadlock 회피)
  RESERVE_MULTI 처리 순서
    1) 좌석 수 2~4, 범위 1~100 검사
    2) 좌석 번호 오름차순 정렬 ([5,3] -> [3,5]), 중복 검사
    3) 정렬된 순서대로 Lock 획득
    4) 전부 비었으면 전부 배정, 아니면 아무것도 안 바꾸고 FAIL (대기열 등록 안 함)
    5) finally에서 잡은 Lock 전부 해제
  모든 Worker가 작은 번호부터 잡기 때문에 서로 상대 Lock을 기다리는 순환이 생기지 않음.
  Lock을 2개 이상 잡는 곳은 여기뿐이고 나머지는 한 번에 하나만 잡음.
  Deadlock 횟수: 큐에 요청이 있는데 30초 동안 처리가 하나도 안 되면 1회로 셈 (감시만 함).
  장점: 정렬만 하면 돼서 간단하고 타임아웃·강제 종료가 필요 없음.
  단점: 필요한 Lock을 미리 다 알아야 함. 앞 좌석 Lock을 쥔 채 뒤 좌석을 기다리는 동안 다른 요청도 앞 좌석에서 기다리게 됨.


8. Waitlist / Condition Variable / Notifier
  좌석마다 ArrayDeque 대기열 (FIFO). 단일 RESERVE에서 남의 좌석이면 맨 뒤에 넣고 바로 WAITLISTED 응답.
  Worker는 좌석이 빌 때까지 기다리지 않고 다음 요청으로 넘어감.
  CANCEL 때 좌석 Lock 안에서 대기열 맨 앞(pollFirst)에게 바로 배정함. 좌석이 비어 있는 순간이 없어서
  다른 Client가 끼어들 수 없고, 배정 순서가 등록 순서대로 됨 (FIFO는 NOTIFY 도착 순서가 아니라 배정 순서 기준).
  배정 후 Worker는 Lock을 풀고 Notify Queue에 작업을 넣고 signal. Notifier는 hasWork.await()로 자다가 깨서
  NOTIFY를 보내고 대기시간을 기록함. 종료 시 남은 통지를 다 보내고 끝남.
  Worker 대기(notEmpty)와 Notifier 대기(hasWork) 모두 Condition Variable 사용. sleep 반복 확인 없음.


9. 테스트 및 정합성 확인
  테스트
  - 요청 좌석이 1~10번에 몰리게 해서(실측 약 63%) 같은 좌석 동시 요청을 반복시킴.
    개발 중에는 간격 0~20ms, 인기 좌석 1~5번으로 부하를 더 줘서 여러 번 돌림. 모두 이중예약 0.
  - Client가 다중 예약 좌석을 정렬 안 하고 보냄. 서버 LOCK 로그(seats[받은 순서] -> acquired 오름차순)로
    항상 정렬해서 잡는 것 확인. Deadlock 0.
  - 실행 중 jstack으로 서버 스레드가 Listener 1, Worker 10, Notifier 1뿐인 것 확인.

  정합성 확인 (Verify로 Server.txt와 Client1~30.txt를 비교, 결과는 logs/VerifyResult.txt)
    1) 이중예약 0건
    2) 배정 수 - 해제 수 = 종료 시 예약 좌석 수
    3) 서버 최종 좌석 = Client 30개 최종 보유 좌석 (좌석별로 비교)
    4) WAITLISTED 수 = NOTIFY 수신 수 + 종료 시 미해결 대기 수
    5) 모든 Client가 5,000건 보내고 응답 다 받고 BYE 받음
  집계 기준: 좌석이 누군가에게 배정될 때마다 배정 +1, CANCEL 성공마다 해제 +1
            (대기자에게 넘기는 경우는 해제 1 + 배정 1)

  실측 결과 (원격 구성, 30 x 5,000건)
    처리량                   req/s
    Request Queue 최대 길이     건
    이중예약                    건
    Deadlock                    건
    Waitlist 평균 대기시간      sec
    Lock 경합                   건
    최종 좌석 정합성
    ---
    총 요청        150,000건
    SUCCESS            건 (  %)
    FAIL               건 (  %)
    WAITLISTED         건 (  %)
    NOTIFY / 미해결      건 /   건
    평균 응답 시간      ms

    (참고: 원격 리허설 30 x 100건 - 처리량 47.8 req/s, 큐 최대 12, 이중예약 0, Deadlock 0, Waitlist 대기 13.8s,
     경합 0, PASS, SUCCESS 36.3% / FAIL 45.4% / WAITLISTED 18.2%, 평균 응답 301ms)
  실제 속도(초당 약 50건)에서는 Worker가 동시에 같은 좌석을 잡는 일이 드물어 Lock 경합 수가 작게 나옴.


10. 직접 정한 것들
  - 큐: 크기 1000, 꽉 차면 Listener 대기. 요청을 버리면 15만 건 응답이 안 맞아서.
  - 요청 비율: 좌석 없으면 RESERVE 60 / MULTI 40, 있으면 30 / 20 / CANCEL 50 (명세 권장안).
    보유 좌석이 5개 이상이면 CANCEL 먼저. 좌석이 한 Client에 쌓이는 걸 막으려고.
  - 인기 좌석: 70% 확률로 1~10번. 60%로 했더니 CANCEL 좌석 때문에 전체 비율이 53%라 50%에 너무 가까워서 올림.
  - 다중 예약: 2~4석 랜덤, 겹치지 않게 고르고 정렬 안 한 순서로 보냄.
  - Client 상태: SUCCESS/NOTIFY로 받은 좌석만 보유로 침. 응답 기다리는 요청은 reqId별로 저장.
    취소 응답 기다리는 좌석은 다시 요청 안 함 (두 응답이 반대로 와서 보유 상태가 틀어지는 걸 Verify로 발견해서 막음).
  - 연결 끊김: 재접속 안 함. 중간에 끊기면 그 실행은 무효로 보고 다시 돌림. 전원 끊기면 서버가 알아서 종료.
  - 로그 용량: Server.txt 약 40MB, Client 로그 합 약 26MB. 줄이지 않고 zip으로 압축 (약 7MB).
  - 화면에는 시작, 전원 접속, 경고, 최종 결과만 출력하고 나머지는 파일에만 기록.
  - 영상: 원격 서버 실행과 Client 30개 접속 -> 다중 예약 로그 -> 종료 후 Verify PASS 순서.


11. 기타
  - Verify.java로 정합성 자동 확인
  - Client는 서버보다 먼저 켜도 1초 간격 30번 접속 재시도
  - 이상한 메시지가 와도 서버가 죽지 않고 FAIL 처리, Worker에서 예외가 나도 응답은 보냄
  - Client가 응답을 안 읽어서 송신이 10초 넘게 막히면 연결을 닫음


12. 제출물 (G__HW2.zip)
  src/, Readme.txt, AllDefinedLogs.txt, logs/Server.txt, logs/Client1~30.txt, logs/VerifyResult.txt, download.txt
