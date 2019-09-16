package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"net/http"
)

type GetClusterResponse struct {
	LeaderAddr string `json:"LeaderAddr"`
}

func GetClusterInfo(host string) (string, error) {
	// TODO: pass multiple hosts, and retry to find one
	getClusterUrl := "http://" + host + "/admin/getCluster"
	glog.V(2).Infof("CFS: getCluster url:%v", getClusterUrl)

	resp, err := http.Get(getClusterUrl)
	if err != nil {
		glog.Errorf("Get cfs Cluster info failed, error:%v", err)
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Read response of getCluster is failed. err:%v", err)
		return "", err
	}
	if resp.StatusCode == http.StatusBadRequest {
		glog.Error(string(body))
		return "", fmt.Errorf(string(body))
	}

	var cfsClusterResp = &GetClusterResponse{}
	if err := json.Unmarshal(body, cfsClusterResp); err != nil {
		glog.Errorf("Cannot unmarshal response of getCluster. bodyLen:%d, err:%v", len(body), err)
		return "", err
	}
	glog.V(2).Infof("CFS: getCluster response:%v", cfsClusterResp)
	if cfsClusterResp.LeaderAddr == "" {
		glog.Errorf("cluster no leader.")
		return "", err
	}
	return cfsClusterResp.LeaderAddr, nil
}

func CreateVolume(host string, volumeName string, volSizeGB int) error {
	createVolUrl := fmt.Sprintf("http://%s/admin/createVol?name=%s&capacity=%v&owner=cfs&replicas=3&type=extent", host, volumeName, volSizeGB)
	glog.V(2).Infof("CFS: CreateVol url:%v", createVolUrl)

	resp, err := http.Get(createVolUrl)
	if err != nil {
		glog.Errorf("CreateVol cfs failed, error:%v", err)
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Read response of createVol is failed. err:%v", err)
		return err
	}

	glog.V(2).Infof("CFS: createVol response:%v", string(body))

	if resp.StatusCode == http.StatusBadRequest {
		glog.Errorf("CFS: create volume is failed. msg:%v", string(body))
		return fmt.Errorf("create volume is failed")
	}
	return nil
}

func DeleteVolume(host string, volumeName string) error {
	deleteVolUrl := "http://" + host + "/vol/delete?name=" + volumeName + "&authKey=7b2f1bf38b87d32470c4557c7ff02e75"
	resp, err := http.Get(deleteVolUrl)
	if err != nil {
		glog.Errorf("DeleteVol cfs failed, error:%v", err)
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Read response of deleteVol is failed. err:%v", err)
		return err
	}

	glog.V(2).Infof("CFS: delete volume response:%v", string(body))

	if resp.StatusCode == http.StatusBadRequest {
		glog.Errorf("CFS: delete volume is failed. msg:%v", string(body))
		return fmt.Errorf("delete volume is failed")
	}
	return nil
}
