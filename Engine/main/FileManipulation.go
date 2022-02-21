package main

import (
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func createFile(putanja string) *os.File {
	file, err := os.Create(putanja)
	if err != nil {
		panic(err)
	}
	file.Close()
	return file
}
func deleteFile(putanja string) {

	err := os.Remove(putanja)
	if err != nil {
		panic(err)
	}
}

func resetWal(directoryName string) bool {

	deleteDirectory(directoryName)
	createFile(directoryName + "wal_0001.log")
	return true
}

func deleteDirectory(directoryName string) bool {
	filenames := readDirectory(directoryName)
	for _, i := range filenames {
		deleteFile(i)
	}
	return true
}

func getFileIndex(filename string) (prvi string, drugi string) {
	//usertable-data-ic-1-1-Filter.db
	dashCounter := 0

	prvi = ""
	drugi = ""

	for i := 0; i < len(filename); i++ {
		if dashCounter == 3 {
			prvi += string(filename[i])
		}
		if dashCounter == 4 {
			drugi += string(filename[i])
		}
		if filename[i] == '-' {
			dashCounter++
		}

	}
	prvi = prvi[:len(prvi)-1]
	drugi = drugi[:len(drugi)-1]

	return
}

func getCreationTime(filename string) time.Time {
	finfo, _ := os.Stat(filename)

	d := finfo.Sys().(*syscall.Win32FileAttributeData)
	cTime := time.Unix(0, d.CreationTime.Nanoseconds())
	return cTime

}

func sortByCreationTime(filenames []string) []string {
	swap := reflect.Swapper(filenames)

	for i := 0; i < len(filenames); i++ {
		for j := i; j < len(filenames); j++ {
			if getCreationTime(filenames[i]).Before(getCreationTime(filenames[j])) {
				swap(i, j)
			}
		}
	}

	return filenames
}

func getByGeneration(filenames []string, index uint64) []string {
	names := make([]string, 0)
	for i := 0; i < len(filenames); i++ {
		_, drugi := getFileIndex(filenames[i])
		if drugi == strconv.FormatUint(index, 10) {
			names = append(names, filenames[i])

		}

	}
	return names

}

func setNewFilenameBasedOnOffsets(offsets []string) string {
	newFilename := "wal/wal_"
	if len(offsets) == 0 {
		newFilename += "0001.log"
		return newFilename
	}
	last_offset := offsets[len(offsets)-1]
	intOffset, err := strconv.Atoi(last_offset)
	if err != nil {
		panic(err)
	}
	if intOffset < 9 {
		newFilename += "000" + strconv.Itoa(intOffset+1)
	} else if intOffset < 99 {
		newFilename += "00" + strconv.Itoa(intOffset+1)
	} else if intOffset < 999 {
		newFilename += "0" + strconv.Itoa(intOffset+1)
	} else {
		newFilename += strconv.Itoa(intOffset + 1)
	}
	newFilename += ".log"

	return newFilename
}

func basedOnIndexFilename(index int) string {
	filename := "wal/wal_"
	if index < 10 {
		filename += "000" + strconv.Itoa(index+1)
	} else if index < 100 {
		filename += "00" + strconv.Itoa(index+1)
	} else if index < 1000 {
		filename += "0" + strconv.Itoa(index+1)
	} else {
		filename += strconv.Itoa(index + 1)
	}
	filename += ".log"

	return filename
}

func existsFile(putanja string) bool {
	if _, err := os.Stat(putanja); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
func renameFile(filename, newName string) {
	err := os.Rename(filename, newName)
	if err != nil {
		panic(err)
	}
}

func splitOffests(filenames []string) []string {
	// vraca samo offsete 0001, 0002..
	offsets := make([]string, len(filenames))
	for i, j := range filenames {
		s := strings.Split(j, "_")
		finalSplit := strings.Split(s[1], ".")
		offsets[i] = finalSplit[0]
	}
	return offsets
}

func readDirectory(putanja string) []string {
	if _, err := os.Stat(putanja); os.IsNotExist(err) {
		if err := os.Mkdir(putanja, os.ModePerm); err != nil {
			log.Fatal(err)
		}
	}
	files, err := ioutil.ReadDir(putanja)
	if err != nil {
		return nil
	}
	fileNames := make([]string, len(files))
	for i := 0; i < len(files); i++ {
		fileNames[i] = putanja + "/" + files[i].Name()
	}
	return fileNames
}

func createHeaderData(key string) []byte {
	dataToReturn := make([]byte, 0)

	keyLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLength, uint64(len(key)))

	dataToReturn = append(dataToReturn, keyLength...)
	dataToReturn = append(dataToReturn, []byte(key)...)

	return dataToReturn

}

func createIndexData(key string, pointerPosition uint64) []byte {

	keyLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLength, uint64(len(key)))

	// 8B za kljuc
	// 8B za poziciju

	dataToReturn := make([]byte, 0)

	// dodavanje duzina kljuca i pokazivaca
	dataToReturn = append(dataToReturn, keyLength...)
	//dataToReturn = append(dataToReturn, pointerPositionLength...)

	// dodavanje kljuca
	dataToReturn = append(dataToReturn, []byte(key)...)

	// dodavanje pokazivaca
	//pointerPositionArr := make([]byte, 8)
	//binary.LittleEndian.PutUint64(pointerPositionArr, pointerPosition)
	bs := make([]byte, 8)
	// 0000007
	binary.LittleEndian.PutUint64(bs, pointerPosition)
	dataToReturn = append(dataToReturn, bs...)

	return dataToReturn

}

func createWalData(key string, value []byte, tombstone byte, timestamp uint64) []byte {

	crcByte := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcByte, CRC32(value))

	timeNowByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeNowByte, timestamp)

	tsByte := make([]byte, 0)
	tsByte = append(tsByte, tombstone)

	keyLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLength, uint64(len(key)))

	valueLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLength, uint64(len(value)))

	dataToReturn := make([]byte, 0)

	dataToReturn = append(dataToReturn, crcByte...)
	dataToReturn = append(dataToReturn, timeNowByte...)
	dataToReturn = append(dataToReturn, tsByte...)
	dataToReturn = append(dataToReturn, keyLength...)
	dataToReturn = append(dataToReturn, valueLength...)
	dataToReturn = append(dataToReturn, []byte(key)...)
	dataToReturn = append(dataToReturn, value...)
	return dataToReturn

}

func appendData(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}

	mmapf, err := mmap.MapRegion(file, int(currentLen)+len(data), mmap.RDWR, 0, 0)
	if err != nil {
		return err
	}

	defer mmapf.Unmap()
	copy(mmapf[currentLen:], data)
	mmapf.Flush()
	return nil
}


func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}