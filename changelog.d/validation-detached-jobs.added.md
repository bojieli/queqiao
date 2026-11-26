Live validation campaigns keep running their test jobs through a dropped
connection to a test host, and a block whose job the harness itself loses is
set aside for both arms instead of leaving the whole campaign without a
verdict; failures inside the tunnel still count against the arm.
