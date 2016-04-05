------------------------------- MODULE rousseau -------------------------------
EXTENDS Naturals, TLC
CONSTANT N

CONCERNS(selfx, resx) == (resx - 1) % N = (selfx - 1)
TCONCERNS(selfx, res, TTT) == \E d \in TTT[res].dep: CONCERNS(selfx, d)
OCONCERNS(selfx, res, TTT) == \E d \in TTT[res].dep \cup TTT[res].new: CONCERNS(selfx, d)


(*

--fair algorithm RousseauCommit {
  variables 
  
    (* Define this specific scenario. *)
    t1 = [dep |-> {1,2}, new |-> {5}];
    t2 = [dep |-> {2,3}, new |-> {6}];
    t3 = [dep |-> {5,3}, new |-> {7}];
    t4 = [dep |-> {6,1}, new |-> {8}];
    transactions = <<t1, t2, t3, t4>>;
    initial_alloc = (1 :> {1}) @@ (2 :> {2}) @@ (3 :> {3});

    (* Data structures for all scenarios. *)    
    shards = 1..N;
    (* Abstraction: Resources in the set "exist_resources" are those for which at least one
                    node has accepted the transaction in which they are created. Thus it
                    abstracts a valid signature.                                            
    *)
    exist_resources = UNION {initial_alloc[i] : i \in DOMAIN initial_alloc };
    TTT = [t \in DOMAIN transactions |-> transactions[t]];
    resources = [res \in shards |-> (IF res \in DOMAIN initial_alloc THEN initial_alloc[res] ELSE {})];
    accepted = [acc \in shards |-> {}];
    rejected = [rej \in shards |-> {}];
    votes = [vot \in shards |-> {}];
    
    proc_chan = {<<"process", c>> : c \in DOMAIN TTT};
    votes_chan = {};
    commit_chan = {};

  (* Sending is simply adding to a set: thus messages can arrive many times, and out of order. *)
  macro send(chan, msgs){
    chan := chan \cup msgs;
  }  
  

  process (node \in shards)
  {
    c_start:+ await (proc_chan # {});
    c_loop:+ while ({p \in proc_chan: (p[2] \notin accepted[self] \cup rejected[self]) 
                    /\  TTT[p[2]].dep \subseteq exist_resources } # {})
                    (* /\ OCONCERNS(self, p[2], TTT)} # {}) *)
    {
    
            with(proc_node = {p \in proc_chan: 
                                p[2] \notin accepted[self] \cup rejected[self] 
                                /\ TCONCERNS(self, p[2], TTT)})
            if (proc_node # {}) with (m \in proc_node)
            (* Abstraction: transactions point to the source of their dependencies, that each need to 
                            have at least one valid signature. Resources in the "exist_resources"
                            have such a signature. *)
            if(TTT[m[2]].dep \subseteq  exist_resources)
            {                        
                                        
                (* Make sure there are never any contradictory votes. *)                
                assert  (
                LET    my_votes == {v \in votes_chan : v[4] = self /\ v[2] = m[2]}
                       decisions == {v[5] : v \in my_votes }                             
                IN decisions = {} \/ decisions = {"yes"} \/ decisions = {"no"}
                );
                                
                (* Handle transactions to be processed. *)
                with(m_votes = {v \in votes_chan : v[2] = m[2]})
                if ( self \notin { v[4] : v \in m_votes }){
                                                                                          
                    with(good_votes = {v \in votes[self] : (\A iacc \in accepted[self]: v # iacc => TTT[v].dep \intersect TTT[iacc].dep = {})}) 
                    with(used = UNION { TTT[f0].dep: 
                        f0 \in (accepted[self] \cup ( good_votes \ rejected[self]))}){
                        if (TTT[m[2]].dep \intersect resources[self] # {}){
                            
                            (* Make sure that if we still support a vote as valid the transaction
                               does not conflit with any we have accepted so far. *)                                
                            assert \A i \in good_votes \ rejected[self] : (\A j \in accepted[self]: 
                                    i # j => TTT[i].dep \intersect TTT[j].dep = {});
                                                                                                                                                                
                            if (TTT[m[2]].dep \intersect used = {}){
                                send(votes_chan, { <<"vote", m[2], resx, self, "yes">> : resx \in TTT[m[2]].dep \intersect resources[self] } );
                                votes[self] := votes[self] \cup { m[2] };                                 
                            }
                            else {
                                send(votes_chan, { <<"vote", m[2], resx, self, "no">> : resx \in TTT[m[2]].dep \intersect used} );

                                (* Ensure we do not vote "no" after we voted "yes". *)
                                assert m[2] \notin votes[self];
                            };
                        };                                
                    };
                };
            };

            (*// Handle transactions to be committed. *)
            with(active_votes = {p \in votes_chan: p[2] \notin accepted[self] \cup rejected[self]
                                 /\ TCONCERNS(self, p[2], TTT)})
            if (active_votes # {}) with(m \in active_votes)
            {
                (* Count "yes" and "no" votes. *)
                with(m_yes_votes = {v2[3] : v2 \in {v \in votes_chan : (v[2] = m[2] /\ v[5] = "yes")}} )
                with(m_no_votes = {v3[3] : v3 \in{v \in votes_chan : (v[2] = m[2]  /\ v[5] = "no")}} )
                    if (TTT[m[2]].dep \subseteq m_yes_votes){
                        send(commit_chan, { <<"commit", m[2], "yes">> } );                                                                       
                    } 
                    else 
                    if (m_no_votes # {}){
                        send(commit_chan, { <<"commit", m[2], "no">> } );
                    };                
            };

            (* // Do the committing of the transactions. *)        
            with(active_commit = {p \in commit_chan: p[2] \notin accepted[self] \cup rejected[self] (* }) *)
                    /\ OCONCERNS(self, p[2], TTT)})
            if (active_commit # {}) with(m \in active_commit)
            {                                                            
                if (<<"commit", m[2], "yes">> \in commit_chan){                            
                    send(accepted[self], { m[2] } );
                    (* Updates the resources for this node *)  
                    send(resources[self], {newres \in TTT[m[2]].new : CONCERNS(self, newres) });
                    
                    (* Abstraction: add a resource to "exist_resources" is equivalent to saying there exists
                       a valid signature on the transaction that generated it. *)
                    send(exist_resources, TTT[m[2]].new);                      
                }
                else
                if (<<"commit", m[2], "no">> \in commit_chan){                                      
                    send(rejected[self], { m[2] } );                                
                };
                
                (* Set of accepted and rejected transactions should be disjoint. *)
                assert accepted[self] \intersect rejected[self] = {};
            };                    
    };    
    
    (* As much progress as possible was made *)
    assert accepted[self] \cup rejected[self] = {t \in (DOMAIN TTT) : 
                                                  TTT[t].dep \subseteq exist_resources };                                                 
                                                  (* /\ OCONCERNS(self, t, TTT) } ; *) 
    (* At least one transaction was accepted. *)
    (*
    assert accepted[self] # {}; *)       
  }
      
} \* end algorithm

*)


    
\* ================ BEGIN TRANSLATION ================
VARIABLES t1, t2, t3, t4, transactions, initial_alloc, shards, 
          exist_resources, TTT, resources, accepted, rejected, votes, 
          proc_chan, votes_chan, commit_chan, pc

vars == << t1, t2, t3, t4, transactions, initial_alloc, shards, 
           exist_resources, TTT, resources, accepted, rejected, votes, 
           proc_chan, votes_chan, commit_chan, pc >>

ProcSet == (shards)

Init == (* Global variables *)
        /\ t1 = [dep |-> {1,2}, new |-> {5}]
        /\ t2 = [dep |-> {2,3}, new |-> {6}]
        /\ t3 = [dep |-> {5,3}, new |-> {7}]
        /\ t4 = [dep |-> {6,1}, new |-> {8}]
        /\ transactions = <<t1, t2, t3, t4>>
        /\ initial_alloc = (1 :> {1}) @@ (2 :> {2}) @@ (3 :> {3})
        /\ shards = 1..N
        /\ exist_resources = UNION {initial_alloc[i] : i \in DOMAIN initial_alloc }
        /\ TTT = [t \in DOMAIN transactions |-> transactions[t]]
        /\ resources = [res \in shards |-> (IF res \in DOMAIN initial_alloc THEN initial_alloc[res] ELSE {})]
        /\ accepted = [acc \in shards |-> {}]
        /\ rejected = [rej \in shards |-> {}]
        /\ votes = [vot \in shards |-> {}]
        /\ proc_chan = {<<"process", c>> : c \in DOMAIN TTT}
        /\ votes_chan = {}
        /\ commit_chan = {}
        /\ pc = [self \in ProcSet |-> "c_start"]

c_start(self) == /\ pc[self] = "c_start"
                 /\ (proc_chan # {})
                 /\ pc' = [pc EXCEPT ![self] = "c_loop"]
                 /\ UNCHANGED << t1, t2, t3, t4, transactions, initial_alloc, 
                                 shards, exist_resources, TTT, resources, 
                                 accepted, rejected, votes, proc_chan, 
                                 votes_chan, commit_chan >>

c_loop(self) == /\ pc[self] = "c_loop"
                /\ IF {p \in proc_chan: (p[2] \notin accepted[self] \cup rejected[self])
                      /\  TTT[p[2]].dep \subseteq exist_resources } # {}
                      THEN /\ LET proc_node == {p \in proc_chan:
                                                  p[2] \notin accepted[self] \cup rejected[self]
                                                  /\ TCONCERNS(self, p[2], TTT)} IN
                                IF proc_node # {}
                                   THEN /\ \E m \in proc_node:
                                             IF TTT[m[2]].dep \subseteq  exist_resources
                                                THEN /\ Assert(        (
                                                               LET    my_votes == {v \in votes_chan : v[4] = self /\ v[2] = m[2]}
                                                                      decisions == {v[5] : v \in my_votes }
                                                               IN decisions = {} \/ decisions = {"yes"} \/ decisions = {"no"}
                                                               ), 
                                                               "Failure of assertion at line 65, column 17.")
                                                     /\ LET m_votes == {v \in votes_chan : v[2] = m[2]} IN
                                                          IF self \notin { v[4] : v \in m_votes }
                                                             THEN /\ LET good_votes == {v \in votes[self] : (\A iacc \in accepted[self]: v # iacc => TTT[v].dep \intersect TTT[iacc].dep = {})} IN
                                                                       LET used ==         UNION { TTT[f0].dep:
                                                                                   f0 \in (accepted[self] \cup ( good_votes \ rejected[self]))} IN
                                                                         IF TTT[m[2]].dep \intersect resources[self] # {}
                                                                            THEN /\ Assert(\A i \in good_votes \ rejected[self] : (\A j \in accepted[self]:
                                                                                            i # j => TTT[i].dep \intersect TTT[j].dep = {}), 
                                                                                           "Failure of assertion at line 82, column 29.")
                                                                                 /\ IF TTT[m[2]].dep \intersect used = {}
                                                                                       THEN /\ votes_chan' = (votes_chan \cup ({ <<"vote", m[2], resx, self, "yes">> : resx \in TTT[m[2]].dep \intersect resources[self] }))
                                                                                            /\ votes' = [votes EXCEPT ![self] = votes[self] \cup { m[2] }]
                                                                                       ELSE /\ votes_chan' = (votes_chan \cup ({ <<"vote", m[2], resx, self, "no">> : resx \in TTT[m[2]].dep \intersect used}))
                                                                                            /\ Assert(m[2] \notin votes[self], 
                                                                                                      "Failure of assertion at line 93, column 33.")
                                                                                            /\ votes' = votes
                                                                            ELSE /\ TRUE
                                                                                 /\ UNCHANGED << votes, 
                                                                                                 votes_chan >>
                                                             ELSE /\ TRUE
                                                                  /\ UNCHANGED << votes, 
                                                                                  votes_chan >>
                                                ELSE /\ TRUE
                                                     /\ UNCHANGED << votes, 
                                                                     votes_chan >>
                                   ELSE /\ TRUE
                                        /\ UNCHANGED << votes, votes_chan >>
                           /\ LET active_votes == {p \in votes_chan': p[2] \notin accepted[self] \cup rejected[self]
                                                   /\ TCONCERNS(self, p[2], TTT)} IN
                                IF active_votes # {}
                                   THEN /\ \E m \in active_votes:
                                             LET m_yes_votes == {v2[3] : v2 \in {v \in votes_chan' : (v[2] = m[2] /\ v[5] = "yes")}} IN
                                               LET m_no_votes == {v3[3] : v3 \in{v \in votes_chan' : (v[2] = m[2]  /\ v[5] = "no")}} IN
                                                 IF TTT[m[2]].dep \subseteq m_yes_votes
                                                    THEN /\ commit_chan' = (commit_chan \cup ({ <<"commit", m[2], "yes">> }))
                                                    ELSE /\ IF m_no_votes # {}
                                                               THEN /\ commit_chan' = (commit_chan \cup ({ <<"commit", m[2], "no">> }))
                                                               ELSE /\ TRUE
                                                                    /\ UNCHANGED commit_chan
                                   ELSE /\ TRUE
                                        /\ UNCHANGED commit_chan
                           /\ LET active_commit ==              {p \in commit_chan': p[2] \notin accepted[self] \cup rejected[self]
                                                   /\ OCONCERNS(self, p[2], TTT)} IN
                                IF active_commit # {}
                                   THEN /\ \E m \in active_commit:
                                             /\ IF <<"commit", m[2], "yes">> \in commit_chan'
                                                   THEN /\ accepted' = [accepted EXCEPT ![self] = (accepted[self]) \cup ({ m[2] })]
                                                        /\ resources' = [resources EXCEPT ![self] = (resources[self]) \cup ({newres \in TTT[m[2]].new : CONCERNS(self, newres) })]
                                                        /\ exist_resources' = (exist_resources \cup (TTT[m[2]].new))
                                                        /\ UNCHANGED rejected
                                                   ELSE /\ IF <<"commit", m[2], "no">> \in commit_chan'
                                                              THEN /\ rejected' = [rejected EXCEPT ![self] = (rejected[self]) \cup ({ m[2] })]
                                                              ELSE /\ TRUE
                                                                   /\ UNCHANGED rejected
                                                        /\ UNCHANGED << exist_resources, 
                                                                        resources, 
                                                                        accepted >>
                                             /\ Assert(accepted'[self] \intersect rejected'[self] = {}, 
                                                       "Failure of assertion at line 137, column 17.")
                                   ELSE /\ TRUE
                                        /\ UNCHANGED << exist_resources, 
                                                        resources, accepted, 
                                                        rejected >>
                           /\ pc' = [pc EXCEPT ![self] = "c_loop"]
                      ELSE /\ Assert(accepted[self] \cup rejected[self] = {t \in (DOMAIN TTT) :
                                                                            TTT[t].dep \subseteq exist_resources }, 
                                     "Failure of assertion at line 142, column 5.")
                           /\ pc' = [pc EXCEPT ![self] = "Done"]
                           /\ UNCHANGED << exist_resources, resources, 
                                           accepted, rejected, votes, 
                                           votes_chan, commit_chan >>
                /\ UNCHANGED << t1, t2, t3, t4, transactions, initial_alloc, 
                                shards, TTT, proc_chan >>

node(self) == c_start(self) \/ c_loop(self)

Next == (\E self \in shards: node(self))
           \/ (* Disjunct to prevent deadlock on termination *)
              ((\A self \in ProcSet: pc[self] = "Done") /\ UNCHANGED vars)

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Next)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

(* Completeness == <>(\A selfx \in shards: accepted[selfx] \cup rejected[selfx] = DOMAIN TTT ) *)

(* Consensus == <>(\A self1 \in shards: \A self2 \in shards: (accepted[self1] = accepted[self2] 
                /\ rejected[self1] = rejected[self2] /\ accepted[self1] # rejected[self2]))

*)

(* Key temporal formulas *)

(* Helpful definitions / functions *)
UN(set_list, selfi, Tx) == { t \in UNION { set_list[u] : u \in DOMAIN set_list }: OCONCERNS(selfi, t, Tx) }
ALLT(acc, rej) == UNION ({acc[s]: s \in DOMAIN acc} \cup {rej[s]: s \in DOMAIN rej})
ALLACC(acc) == UNION ({acc[s]: s \in DOMAIN acc})

(* All shards learn about the accepted and rejected facts that concern them. *)
ConsensusX == <>(\A selfi \in shards: accepted[selfi] = UN(accepted, selfi, TTT) 
                                     /\ rejected[selfi] = UN(rejected, selfi, TTT) )

(* Ensure that when transactions are not processed, it is because some inputs are unknown. *)
UnknownNotProcessed == <>(\A p \in proc_chan: p[2] \notin ALLT(accepted, rejected) => TTT[p[2]].dep \ exist_resources # {})

(* Ensure that at least some transactions were processed, thus progress was made. *)
SomeProgress == <>(ALLACC(accepted) # {})

=============================================================================
\* Modification History
\* Last modified Tue Apr 05 23:57:22 BST 2016 by george
\* Created Fri Apr 01 23:45:07 BST 2016 by george
