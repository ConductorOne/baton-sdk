package connectorbuilder

// FIXME(jirwin): Come back to streaming assets soon.
//
// const (
// 	maxAssetChunkSize = 1048576
// )

// streamAsset is a helper that chunks an asset and streams it to the client. Ensures that the content type is sent first.
// func streamAsset(contentType string, asset io.Reader, server v2.AssetService_GetAssetServer) error {
// 	err := server.Send(&v2.AssetServiceGetAssetResponse{
// 		Msg: &v2.AssetServiceGetAssetResponse_Metadata_{
// 			Metadata: &v2.AssetServiceGetAssetResponse_Metadata{ContentType: contentType},
// 		},
// 	})
// 	if err != nil {
// 		return err
// 	}
//
// 	for {
// 		buf := &bytes.Buffer{}
// 		n, copyErr := io.CopyN(buf, asset, maxAssetChunkSize)
// 		if copyErr != nil && !errors.Is(copyErr, io.EOF) {
// 			return copyErr
// 		}
//
// 		if n > 0 {
// 			err = server.Send(&v2.AssetServiceGetAssetResponse{
// 				Msg: &v2.AssetServiceGetAssetResponse_Data_{
// 					Data: &v2.AssetServiceGetAssetResponse_Data{Data: buf.Bytes()},
// 				},
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		if errors.Is(copyErr, io.EOF) {
// 			break
// 		}
// 	}
// 	return nil
// }
