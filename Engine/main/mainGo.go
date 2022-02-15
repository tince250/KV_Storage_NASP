package main

func main(){
	/*
		1. Ucitavamo default vrednosti za memtable i wal 					GOTOVO
		2. Kreiramo wal segment, ukoliko ne postoji wal pravimo ga			!!!!!!
		3. Kreiramo skip listu, ucitamo je u memoriju 						GOTOVO
		4. Postavljamo default vrednosti za velicinu i segment				GOTOVO
		5. Upisivanje u memtable moze poceti!								GOTOVO
		6. Kad dodje do tresholda, ide flush								!!!!!!
		7. Rekreacija wal-a - ucitavamo redom podatke iz walova 			GOTOVO
			(od prvog do poslednjeg), i redom
			primenjujemo izmene na memtable
	*/
	defVals := &defValues{}
	defVals.getDefaultValues("config/config.yml")
	sl := &SkipList{}
	sl.createSkipList(defVals.MaxHeight, defVals.Threshold)
	a := readFullData("wal/wal_0001.log")
	sl.inserFromWal(a)


	sl.addNode("marko", []byte("markovic"))
	//sl.addNode("mirko", []byte("cepes"))
	//sl.addNode("Nenad", []byte("Rozic"))
	//sl.addNode("Aleksandar", []byte("Miric"))
	//sl.addNode("Milan", []byte("Miric"))
	//sl.addNode("Dejan", []byte("Miric"))
	sl.addNode("Borko", []byte("Miric"))
	//sl.addNode("Borjan", []byte("Mirkovic"))
	sl.logicalDelete("Borko")

	sl.printList()



}
