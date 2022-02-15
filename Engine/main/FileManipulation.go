package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func createFile(putanja string) *os.File{
	file, err := os.Create(putanja)
	if err!=nil{
		panic(err)
	}
	return file
}
func deleteFile(putanja string) {

	err := os.Remove(putanja)
	if err!=nil{
		panic(err)
	}
}

func setNewFilenameBasedOnOffsets(offsets []string) string{
	newFilename := "wal/wal_"
	if len(offsets) == 0{
		newFilename += "0001.log"
		return newFilename
	}
	last_offset := offsets[len(offsets) - 1]
	intOffset, err := strconv.Atoi(last_offset)
	if err != nil{
		panic(err)
	}
	if intOffset < 9 {
		newFilename += "000" + strconv.Itoa(intOffset+1)
	}else if intOffset < 99 {
		newFilename += "00" + strconv.Itoa(intOffset+1)
	}else if intOffset < 999{
		newFilename += "0" + strconv.Itoa(intOffset+1)
	}else{
		newFilename += strconv.Itoa(intOffset + 1)
	}
	newFilename += ".log"

	return newFilename
}

func deleteAndRenameOldLogsUntilLast(filenames []string) bool{
	for i:=0;i<len(filenames) - 1;i++{
		deleteFile(filenames[i])
	}
	new_filenames := readDirectory("wal/")
	for i:=0;i<len(new_filenames);i++{
		renameFile(new_filenames[i], basedOnIndexFilename(i))
	}
	return true
}

func deleteAndRenameOldLogs(limit string) bool{
	fileNames := readDirectory("wal/")
	offsets := splitOffests(fileNames)
	for i:=0;i<len(offsets);i++{
		if offsets[i] < limit{
			deleteFile(fileNames[i])
		}
	}
	new_filenames := readDirectory("wal/")
	for i:=0;i<len(new_filenames);i++{
		renameFile(new_filenames[i], basedOnIndexFilename(i))
	}
	return true
}

func basedOnIndexFilename(index int) string{
	filename:= "wal/wal_"
	if index < 10 {
		filename += "000" + strconv.Itoa(index+1)
	}else if index < 100{
		filename += "00" + strconv.Itoa(index+1)
	}else if index < 1000{
		filename += "0" + strconv.Itoa(index+1)
	}else{
		filename += strconv.Itoa(index + 1)
	}
	filename += ".log"

	return filename
}

func existsFile(putanja string) bool{
	if _, err := os.Stat(putanja);err!=nil{
		if os.IsNotExist(err){
			return false
		}
	}
	return true
}
func renameFile(filename, newName string){
	err:= os.Rename(filename, newName)
	if err!=nil{
		panic(err)
	}
}
func fileInfo(putanja string){
	file, err := os.Stat(putanja)
	if err !=nil{
		panic(err)
	}
	fmt.Println("Name: ", file.Name())
	fmt.Println("Size", file.Size())
	fmt.Println("Permision", file.Mode())
	fmt.Println("Last modified", file.ModTime())
	fmt.Println("Is directory? ", file.IsDir())
}

func copyToFile(path1, path2 string){
	original, err := os.Open(path1)
	if err!= nil{
		log.Fatal(err)
	}

	newFile, err := os.OpenFile(path2, os.O_WRONLY, 0666)

	if err!=nil{
		log.Fatal(err)
	}


	bytesWritten, err := io.Copy(newFile, original)
	if err !=nil{
		log.Fatal(err)
	}
	log.Printf("Copied %d bytes", bytesWritten)
	err = newFile.Sync()
	if err != nil{
		log.Fatal(err)
	}
	original.Close()
	newFile.Close()
}

func readToStringFromFile(putanja string) []string{
	file, err := os.Open(putanja)
	if err != nil{
		panic(err)
	}
	lines := make([]string, 0)
	scanner := bufio.NewScanner(file)

	for scanner.Scan(){
		line:= scanner.Text()
		lines = append(lines, line)
	}


	file.Close()
	return lines
}

func splitOffests(filenames []string) []string{
	// vraca samo offsete 0001, 0002..
	offsets := make([]string, len(filenames))
	for i, j:= range filenames{
		s:= strings.Split(j, "_")
		finalSplit := strings.Split(s[1], ".")
		offsets[i] = finalSplit[0]
	}
	return offsets
}

func readDirectory(putanja string) []string{
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
	for i:=0;i<len(files);i++{
		fileNames[i] = putanja + "" + files[i].Name()
	}
	return fileNames
}

func createWalData(key string, value []byte, tombstone byte) []byte{

	crcByte:= make([]byte, 4)
	binary.LittleEndian.PutUint32(crcByte, CRC32(value))

	timeNow := time.Now().Unix()
	timeNowByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeNowByte, uint64(timeNow))

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

// Map maps an entire file into memory

// prot argument
// mmap.RDONLY - Maps the memory read-only. Attempts to write to the MMap object will result in undefined behavior.
// mmap.RDWR - Maps the memory as read-write. Writes to the MMap object will update the underlying file.
// mmap.COPY - Writes to the MMap object will affect memory, but the underlying file will remain unchanged.
// mmap.EXEC - The mapped memory is marked as executable.

// flag argument
// mmap.ANON - The mapped memory will not be backed by a file. If ANON is set in flags, f is ignored.
func read(file *os.File) ([]byte, error) {
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()
	result := make([]byte, len(mmapf))
	copy(result, mmapf)
	return result, nil
}

func readRange(file *os.File, startIndex, endIndex int) ([]byte, error) {
	if startIndex < 0 || endIndex < 0 || startIndex > endIndex {
		return nil, errors.New("indices invalid")
	}
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()
	if startIndex >= len(mmapf) || endIndex > len(mmapf) {
		return nil, errors.New("indices invalid")
	}
	result := make([]byte, endIndex-startIndex)
	copy(result, mmapf[startIndex:endIndex])
	return result, nil
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}