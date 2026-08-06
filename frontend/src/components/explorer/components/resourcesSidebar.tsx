import React, { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Back } from "../../icons/icons";
import { CircularProgress, Divider, InputAdornment, TextField, Typography } from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import ClearIcon from "@mui/icons-material/Clear";
import { List, type RowComponentProps } from "react-window";
import { useInfiniteLoader } from "react-window-infinite-loader";
import pluralize from "pluralize";
import { normalizeString } from "../../../common/helpers";
import {
  ResourceLabel,
  Sidebar,
  SidebarHeader,
  StyledButton,
  EmptyResourceLabel,
} from "../styles/styles";
import { useResources } from "../../../context/resources";

const ITEM_HEIGHT = 48;

type SidebarRowProps = {
  filteredResources: any[];
  hasMore: boolean;
  selectedResourceId: string | null;
  onItemClick: (resource: any) => Promise<void>;
};

function SidebarRow({
  index,
  style,
  filteredResources,
  hasMore,
  selectedResourceId,
  onItemClick,
}: RowComponentProps<SidebarRowProps>) {
  if (hasMore && index >= filteredResources.length) {
    return (
      <div style={{ ...style, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <CircularProgress color="success" size={20} />
      </div>
    );
  }

  const resource = filteredResources[index];
  return (
    <div style={style}>
      <ResourceLabel
        disableGutters
        selected={selectedResourceId === resource.resource.id.resource}
        onClick={async () => await onItemClick(resource)}
      >
        <Typography color="inherit">
          {resource.resource.display_name}
        </Typography>
      </ResourceLabel>
    </div>
  );
}

export const ResourcesSidebar = ({
  closeResourceList,
  resourceType,
  openTreeView,
}) => {
  const { mappedResources, fetchResourcesByType, fetchResourcePage, getResourceCache, appendResources } = useResources();
  const [selectedResourceId, setSelectedResourceId] = React.useState(null);
  const [resources, setResources] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const nextPageTokenRef = useRef<string | undefined>(undefined);
  const loadingMoreRef = useRef(false);

  useEffect(() => {
    if (!resourceType) return;

    const cached = getResourceCache(resourceType);
    if (cached) {
      setResources(cached.resources);
      nextPageTokenRef.current = cached.nextPageToken;
      setHasMore(!!cached.nextPageToken);
      return;
    }

    if (mappedResources[resourceType]) {
      setResources(mappedResources[resourceType]);
      setHasMore(false);
      return;
    }

    setLoading(true);
    fetchResourcesByType(resourceType)
      .then((data) => {
        setResources(data);
        const cache = getResourceCache(resourceType);
        nextPageTokenRef.current = cache?.nextPageToken;
        setHasMore(!!cache?.nextPageToken);
      })
      .catch(() => {
        setResources([]);
        setHasMore(false);
      })
      .finally(() => setLoading(false));
  }, [resourceType, mappedResources, fetchResourcesByType, getResourceCache]);

  const loadMoreRows = useCallback(async () => {
    if (loadingMoreRef.current || !nextPageTokenRef.current) return;
    loadingMoreRef.current = true;

    try {
      const resp = await fetchResourcePage(resourceType, nextPageTokenRef.current);
      const newResources = resp.data?.resources || [];
      nextPageTokenRef.current = resp.next_page_token;
      setHasMore(!!resp.next_page_token);

      appendResources(resourceType, newResources, resp.next_page_token);
      setResources((prev) => [...prev, ...newResources]);
    } finally {
      loadingMoreRef.current = false;
    }
  }, [resourceType, fetchResourcePage, appendResources]);

  const handleListItemClick = useCallback(async (resource) => {
    setSelectedResourceId(resource.resource.id.resource);
    await openTreeView(resource);
  }, [openTreeView]);

  const filteredResources = useMemo(() => {
    if (!searchQuery.trim()) return resources;
    const q = searchQuery.toLowerCase();
    return resources.filter((r) =>
      r.resource?.display_name?.toLowerCase().includes(q)
    );
  }, [resources, searchQuery]);

  const rowCount = hasMore ? filteredResources.length + 1 : filteredResources.length;
  const isRowLoaded = useCallback(
    (index: number) => !hasMore || index < filteredResources.length,
    [hasMore, filteredResources.length]
  );

  const onRowsRendered = useInfiniteLoader({
    isRowLoaded,
    loadMoreRows,
    rowCount,
    threshold: 10,
  });

  return (
    <Fragment>
      {resourceType && (
        <Sidebar variant="permanent">
          <SidebarHeader>
            <Typography variant="h5" color="inherit">
              {pluralize(normalizeString(resourceType, true))}
            </Typography>
            <StyledButton onClick={closeResourceList}>
              <Back />
            </StyledButton>
          </SidebarHeader>
          <Divider />
          <TextField
            size="small"
            placeholder="Search..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            sx={{
              mx: 1,
              mt: 1,
              mb: 0.5,
              "& .MuiOutlinedInput-root": {
                color: "rgba(255,255,255,0.9)",
                fontSize: "13px",
                backgroundColor: "rgba(255,255,255,0.06)",
                "& fieldset": { borderColor: "rgba(255,255,255,0.12)" },
                "&:hover fieldset": { borderColor: "rgba(255,255,255,0.2)" },
                "&.Mui-focused fieldset": { borderColor: "rgba(255,255,255,0.3)" },
              },
              "& .MuiInputBase-input::placeholder": {
                color: "rgba(255,255,255,0.4)",
                opacity: 1,
              },
            }}
            slotProps={{
              input: {
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon sx={{ color: "rgba(255,255,255,0.4)", fontSize: 16 }} />
                  </InputAdornment>
                ),
                endAdornment: searchQuery ? (
                  <InputAdornment position="end">
                    <ClearIcon
                      sx={{ color: "rgba(255,255,255,0.4)", fontSize: 16, cursor: "pointer" }}
                      onClick={() => setSearchQuery("")}
                    />
                  </InputAdornment>
                ) : null,
              },
            }}
          />
          {searchQuery && hasMore && (
            <Typography variant="caption" sx={{ px: 2, color: "rgba(255,255,255,0.6)" }}>
              More results may exist — scroll to load
            </Typography>
          )}
          {loading ? (
            <CircularProgress color="success" size={24} sx={{ m: 2, alignSelf: "center" }} />
          ) : resources.length > 0 ? (
            <List
              style={{ height: window.innerHeight - 180, width: "100%", marginTop: 20 }}
              rowCount={rowCount}
              rowHeight={ITEM_HEIGHT}
              rowComponent={SidebarRow}
              rowProps={{
                filteredResources,
                hasMore,
                selectedResourceId,
                onItemClick: handleListItemClick,
              }}
              onRowsRendered={onRowsRendered}
            />
          ) : (
            <EmptyResourceLabel>
              No resources
            </EmptyResourceLabel>
          )}
        </Sidebar>
      )}
    </Fragment>
  );
};
